export type Theme = 'light' | 'dark';
export const applyTheme = (theme: Theme) => document.documentElement.dataset.theme = theme;
