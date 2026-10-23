export type ChatCompactionPollScheduler = {
  schedule: (callback: () => void, delayMs: number) => unknown;
  cancel: (handle: unknown) => void;
};

const scheduler: ChatCompactionPollScheduler = {
  schedule: (callback, delayMs) => setTimeout(callback, delayMs),
  cancel: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>)
};

/**
 * Progress is advisory. Never retries the inference POST or controls its lifetime.
 * At most one read is pending. Ending observation only stops scheduling and
 * fences late results: aborting this read could retire chat's shared encrypted
 * session. The configured transport owns its timeout and cancellation policy.
 */
export function startChatCompactionPolling<T>(options: {
  read: () => Promise<T>;
  onStatus: (status: T) => void;
  onUnavailable?: () => void;
  isActive: (status: T) => boolean;
  isCurrent: () => boolean;
  signal: AbortSignal;
  scheduler?: ChatCompactionPollScheduler;
}): () => void {
  const clock = options.scheduler ?? scheduler;
  let stopped = false;
  let timer: unknown;
  let idleDelayMs = 1000;

  const stop = () => {
    if (stopped) return;
    stopped = true;
    if (timer !== undefined) clock.cancel(timer);
    options.signal.removeEventListener("abort", stop);
  };

  const poll = async () => {
    timer = undefined;
    if (stopped || !options.isCurrent()) {
      stop();
      return;
    }
    let delayMs: number;
    try {
      const status = await options.read();
      if (stopped || !options.isCurrent()) {
        stop();
        return;
      }
      const active = options.isActive(status);
      idleDelayMs = active ? 1000 : Math.min(idleDelayMs * 2, 10000);
      delayMs = active ? 1000 : idleDelayMs;
      options.onStatus(status);
    } catch {
      // No raw errors: a transport/provider error can contain private material.
      if (stopped || !options.isCurrent()) {
        stop();
        return;
      }
      // Losing visibility is not evidence that compaction completed. Clear the
      // advisory indicator and end optional reads for this Response, including
      // old-server 404s. Retrying transport failures can churn shared sessions.
      stop();
      options.onUnavailable?.();
      return;
    }
    if (!stopped && options.isCurrent()) {
      timer = clock.schedule(() => void poll(), delayMs);
    } else {
      stop();
    }
  };

  if (options.signal.aborted) {
    stop();
    return stop;
  }
  options.signal.addEventListener("abort", stop, { once: true });
  // Give the POST a chance to register the execution before the first read.
  timer = clock.schedule(() => void poll(), 500);
  return stop;
}
