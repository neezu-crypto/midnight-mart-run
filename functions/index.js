const { initializeApp } = require('firebase-admin/app');
const { getDatabase } = require('firebase-admin/database');
const { onValueWritten } = require('firebase-functions/v2/database');
const { onSchedule } = require('firebase-functions/v2/scheduler');
const { logger } = require('firebase-functions');

initializeApp();

const DB_INSTANCE = 'midnight-mart-run-24bba-default-rtdb';
const REGION = 'asia-southeast1';

// 방장(host) 클라이언트가 라운드 종료 조건을 감지하면 finalizeRequestedAt만 남기고,
// 실제 정산(누가 탈출/체포됐는지, 점수 계산)은 이 함수가 서버 권한으로 재계산한다.
// 클라이언트가 carriedItems나 status, settlement 점수를 직접 위조해도 반영되지 않는다.
exports.onFinalizeRequested = onValueWritten(
  {
    ref: '/rooms/{roomId}/finalizeRequestedAt',
    instance: DB_INSTANCE,
    region: REGION,
  },
  async (event) => {
    const roomId = event.params.roomId;
    const db = getDatabase();
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
      if (thieves.length === 0) {
        await phaseRef.set('playing');
        return;
      }

      const elapsed = (Date.now() - (timer.startedAt || 0)) / 1000;
      const allDone = thieves.every(([, p]) => p.status !== 'active');
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
