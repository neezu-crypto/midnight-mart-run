const { initializeApp } = require('firebase-admin/app');
const { getDatabase } = require('firebase-admin/database');
const { onValueWritten, onValueDeleted } = require('firebase-functions/v2/database');
const { onSchedule } = require('firebase-functions/v2/scheduler');
const { logger } = require('firebase-functions');

initializeApp();

const DB_INSTANCE = 'midnight-mart-run-24bba-default-rtdb';
const REGION = 'asia-southeast1';
const ARREST_RADIUS = 50;
const APOLOGY_MS = 5000;

// 라운드 종료 조건(전원 탈출/체포 또는 시간 종료)을 서버 상태 기준으로 재확인하고,
// 충족됐으면 정산을 확정한다. finalizeRequestedAt 트리거뿐 아니라 체포 처리 직후,
// 그리고 주기적 스윕에서도 재사용해 "방장 클라이언트가 유일한 트리거"인 상황을 없앤다.
async function tryFinalizeRoom(db, roomId) {
  const phaseRef = db.ref(`rooms/${roomId}/phase`);
  const txResult = await phaseRef.transaction((current) =>
    current === 'playing' ? 'finalizing' : undefined
  );
  if (!txResult.committed) return;

  try {
    const roomSnap = await db.ref(`rooms/${roomId}`).get();
    const room = roomSnap.val();
    if (!room) return;

    const players = room.players || {};
    const items = room.items || {};
    const timer = room.timer || { startedAt: 0, durationSec: 90 };

    const thieves = Object.entries(players).filter(([, p]) => p.role === 'thief');

    const elapsed = (Date.now() - (timer.startedAt || 0)) / 1000;
    // 도둑이 중간에 전원 접속 종료해 0명이 된 경우도 "더 이상 진행할 수 없음"으로 보고
    // 종료 조건을 충족한 것으로 처리한다(그렇지 않으면 시간이 지나도 라운드가 영영 안 끝남).
    const allDone = thieves.length === 0 || thieves.every(([, p]) => p.status !== 'active');
    const timeUp = elapsed >= (timer.durationSec || 90);
    if (!allDone && !timeUp) {
      // 조건 미충족 상태에서 요청됨(조작 시도 혹은 타이밍 어긋남) - 그대로 되돌림
      await phaseRef.set('playing');
      return;
    }

    const updates = {};
    const settlement = {};
    thieves.forEach(([id, p]) => {
      let finalStatus = p.status;
      if (p.status === 'active') {
        finalStatus = 'timeout';
        updates[`rooms/${roomId}/players/${id}/status`] = 'timeout';
        updates[`rooms/${roomId}/players/${id}/carryWeight`] = 0;
        updates[`rooms/${roomId}/players/${id}/carriedItems`] = null;
        Object.keys(p.carriedItems || {}).forEach((itemId) => {
          updates[`rooms/${roomId}/items/${itemId}/state`] = 'returned';
          updates[`rooms/${roomId}/items/${itemId}/carriedBy`] = null;
        });
      }

      let score = 0;
      if (finalStatus === 'escaped') {
        // 클라이언트가 보고한 carriedItems를 그대로 믿지 않고, items 쪽에도
        // 실제로 이 플레이어가 들고 있다고 기록된 항목만 점수로 인정한다.
        score = Object.keys(p.carriedItems || {}).reduce((sum, itemId) => {
          const item = items[itemId];
          const verified = item && item.state === 'carried' && item.carriedBy === id;
          return sum + (verified ? item.value : 0);
        }, 0);
      }
      settlement[id] = { result: finalStatus, score };
    });

    // 이전 라운드의 잔여 정산 데이터가 남지 않도록 settlement 전체를 교체한다.
    updates[`rooms/${roomId}/settlement`] = settlement;
    updates[`rooms/${roomId}/phase`] = 'settled';
    updates[`rooms/${roomId}/settledAt`] = Date.now();
    await db.ref().update(updates);
  } catch (err) {
    logger.error(`room ${roomId} finalize failed, reverting to playing`, err);
    await phaseRef.set('playing').catch(() => {});
    throw err;
  }
}

// 주인(owner) 클라이언트가 스페이스바를 누르면 arrestRequestedAt만 남기고,
// "누가 근처에 있는지", "빈손인지"는 서버가 저장된 실제 위치·소지품으로
// 다시 판정한다. 클라이언트가 자신의 위치/소지품을 위조해 원거리 체포를
// 유도하거나 회피하는 것을 막기 위함이다.
exports.onArrestRequested = onValueWritten(
  {
    ref: '/rooms/{roomId}/arrestRequestedAt',
    instance: DB_INSTANCE,
    region: REGION,
  },
  async (event) => {
    const roomId = event.params.roomId;
    const db = getDatabase();

    const roomSnap = await db.ref(`rooms/${roomId}`).get();
    const room = roomSnap.val();
    if (!room || room.phase !== 'playing') return;

    const players = room.players || {};
    const ownerId = room.hostSessionId;
    const owner = players[ownerId];
    if (!owner || owner.role !== 'owner') return;
    if (owner.frozenUntil && Date.now() < owner.frozenUntil) return;

    let targetId = null;
    let bestDist = Infinity;
    Object.entries(players).forEach(([id, p]) => {
      if (p.role !== 'thief' || p.status !== 'active') return;
      const dx = (p.x || 0) - (owner.x || 0);
      const dy = (p.y || 0) - (owner.y || 0);
      const d = dx * dx + dy * dy;
      if (d < ARREST_RADIUS * ARREST_RADIUS && d < bestDist) {
        bestDist = d;
        targetId = id;
      }
    });
    if (!targetId) return; // 근처에 도둑이 없으면 아무 일도 일어나지 않는다

    const target = players[targetId];
    const updates = {};
    let success;
    if ((target.carryWeight || 0) > 0) {
      success = true;
      updates[`rooms/${roomId}/players/${targetId}/status`] = 'arrested';
      updates[`rooms/${roomId}/players/${targetId}/carryWeight`] = 0;
      updates[`rooms/${roomId}/players/${targetId}/carriedItems`] = null;
      Object.keys(target.carriedItems || {}).forEach((itemId) => {
        updates[`rooms/${roomId}/items/${itemId}/state`] = 'returned';
        updates[`rooms/${roomId}/items/${itemId}/carriedBy`] = null;
      });
    } else {
      success = false;
      updates[`rooms/${roomId}/players/${ownerId}/frozenUntil`] = Date.now() + APOLOGY_MS;
    }
    updates[`rooms/${roomId}/arrestResult`] = {
      targetId,
      nickname: target.nickname || '???',
      success,
      ts: Date.now(),
    };
    await db.ref().update(updates);

    // 이 체포로 남은 도둑이 전부 비활성화됐을 수 있으니 즉시 종료 조건을 재확인한다.
    // 방장 클라이언트가 뒤늦게 감지하기를(탭이 백그라운드에 있는 등) 기다리지 않아도 된다.
    await tryFinalizeRoom(db, roomId).catch((err) =>
      logger.error(`post-arrest finalize check failed for room ${roomId}`, err)
    );
  }
);

// 방장(host) 클라이언트가 라운드 종료 조건을 감지하면 finalizeRequestedAt만 남기고,
// 실제 정산(누가 탈출/체포됐는지, 점수 계산)은 tryFinalizeRoom이 서버 권한으로 재계산한다.
// 클라이언트가 carriedItems나 status, settlement 점수를 직접 위조해도 반영되지 않는다.
exports.onFinalizeRequested = onValueWritten(
  {
    ref: '/rooms/{roomId}/finalizeRequestedAt',
    instance: DB_INSTANCE,
    region: REGION,
  },
  async (event) => {
    await tryFinalizeRoom(getDatabase(), event.params.roomId);
  }
);

// 도둑이 라운드 중 접속을 끊으면 players에서 본인 레코드는 자동 삭제되지만,
// 들고 있던 아이템은 아무도 처리해주지 않으면 영원히 "carried" 상태로 묶여
// 남은 도둑들이 주울 수 없게 된다. 이를 막기 위해 실제로 그 아이템을 들고
// 있었던 게 맞는지 확인한 뒤 바닥으로 되돌린다.
exports.onPlayerRemoved = onValueDeleted(
  {
    ref: '/rooms/{roomId}/players/{sessionId}',
    instance: DB_INSTANCE,
    region: REGION,
  },
  async (event) => {
    const roomId = event.params.roomId;
    const sessionId = event.params.sessionId;
    const before = event.data.val();
    const itemIds = Object.keys((before && before.carriedItems) || {});
    if (itemIds.length === 0) return;

    const db = getDatabase();
    const updates = {};
    await Promise.all(
      itemIds.map(async (itemId) => {
        const snap = await db.ref(`rooms/${roomId}/items/${itemId}`).get();
        const item = snap.val();
        if (item && item.carriedBy === sessionId) {
          updates[`rooms/${roomId}/items/${itemId}/state`] = 'onFloor';
          updates[`rooms/${roomId}/items/${itemId}/carriedBy`] = null;
        }
      })
    );
    if (Object.keys(updates).length > 0) {
      await db.ref().update(updates);
    }
  }
);

// 방장 클라이언트가 시간 종료를 감지해 요청을 보내주는 데만 의존하면, 방장 탭이
// 백그라운드에 있어 감지가 지연/누락될 때 라운드가 끝나지 않는 상태로 남을 수 있다.
// 1분마다 진행 중인 방을 모두 훑어 시간이 다 됐으면 클라이언트 개입 없이 정산한다.
exports.sweepRoundTimeouts = onSchedule(
  { schedule: 'every 1 minutes', region: REGION },
  async () => {
    const db = getDatabase();
    const snap = await db.ref('rooms').orderByChild('phase').equalTo('playing').get();
    if (!snap.exists()) return;

    const now = Date.now();
    const roomIds = [];
    snap.forEach((child) => {
      const room = child.val() || {};
      const timer = room.timer || {};
      const elapsed = (now - (timer.startedAt || 0)) / 1000;
      // 아직 시간이 남은 방은 건드리지 않는다(불필요한 트랜잭션/화면 깜빡임 방지).
      if (elapsed >= (timer.durationSec || 90)) roomIds.push(child.key);
    });
    if (roomIds.length === 0) return;

    await Promise.all(
      roomIds.map((id) =>
        tryFinalizeRoom(db, id).catch((err) => logger.error(`sweep finalize failed for room ${id}`, err))
      )
    );
  }
);

// 방치된 방을 주기적으로 정리한다 - 대기 중인 채 오래 방치된 방, 정산 후 재시작하지
// 않고 남겨진 방, 그 외 원인으로 오래 방치된 모든 방을 대상으로 한다.
exports.cleanupStaleRooms = onSchedule(
  { schedule: 'every 30 minutes', region: REGION },
  async () => {
    const db = getDatabase();
    const now = Date.now();
    const CUTOFF_MIN_MS = 2 * 60 * 60 * 1000; // 조회 대상이 되는 가장 느슨한 기준(정산 후 2시간)
    const WAITING_TTL_MS = 6 * 60 * 60 * 1000; // 대기실에서 6시간 이상 방치
    const SETTLED_TTL_MS = 2 * 60 * 60 * 1000; // 정산 후 2시간 이상 재시작 없음
    const ABSOLUTE_TTL_MS = 24 * 60 * 60 * 1000; // 그 외 어떤 상태든 24시간 이상이면 무조건 정리

    const snap = await db
      .ref('rooms')
      .orderByChild('createdAt')
      .endAt(now - CUTOFF_MIN_MS)
      .get();
    if (!snap.exists()) return;

    const deletions = {};
    snap.forEach((child) => {
      const room = child.val();
      const createdAt = room.createdAt || 0;
      const age = now - createdAt;
      const phase = room.phase;

      let shouldDelete = false;
      if (age > ABSOLUTE_TTL_MS) shouldDelete = true;
      else if (phase === 'waiting' && age > WAITING_TTL_MS) shouldDelete = true;
      else if (phase === 'settled' && age > SETTLED_TTL_MS) shouldDelete = true;

      if (shouldDelete) deletions[child.key] = null;
    });

    const count = Object.keys(deletions).length;
    if (count > 0) {
      await db.ref('rooms').update(deletions);
      logger.log(`cleanupStaleRooms: removed ${count} stale room(s)`);
    }
  }
);
