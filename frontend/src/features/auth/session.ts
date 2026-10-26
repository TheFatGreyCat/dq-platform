let bearer: string | null = null;
let unauthorized: (() => void) | null = null;

export const session = {
  token: () => bearer,
  connect: (token: string) => { bearer = token; },
  activate: () => window.dispatchEvent(new CustomEvent('dq-session-state', { detail: true })),
  disconnect: () => { bearer = null; window.dispatchEvent(new CustomEvent('dq-session-state', { detail: false })); },
  onUnauthorized: (handler: () => void) => { unauthorized = handler; },
  expire: () => unauthorized?.(),
};
