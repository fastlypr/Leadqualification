export function createTaskQueue({ concurrency = 1 } = {}) {
  if (!Number.isInteger(concurrency) || concurrency <= 0) {
    throw new Error("Queue concurrency must be a positive integer.");
  }

  const pending = [];
  const idleWaiters = [];
  let activeCount = 0;

  function resolveIdleIfNeeded() {
    if (activeCount !== 0 || pending.length !== 0) {
      return;
    }

    while (idleWaiters.length) {
      const resolve = idleWaiters.shift();
      resolve();
    }
  }

  function runNext() {
    while (activeCount < concurrency && pending.length > 0) {
      const job = pending.shift();
      activeCount += 1;

      Promise.resolve()
        .then(job.task)
        .then(job.resolve, job.reject)
        .finally(() => {
          activeCount -= 1;
          runNext();
          resolveIdleIfNeeded();
        });
    }

    resolveIdleIfNeeded();
  }

  return {
    add(task) {
      return new Promise((resolve, reject) => {
        pending.push({ task, resolve, reject });
        runNext();
      });
    },
    onIdle() {
      if (activeCount === 0 && pending.length === 0) {
        return Promise.resolve();
      }

      return new Promise((resolve) => {
        idleWaiters.push(resolve);
      });
    },
    size() {
      return pending.length;
    }
  };
}
