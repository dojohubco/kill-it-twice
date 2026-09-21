import { Component, computed, input } from '@angular/core';
const paths: Record<string, string> = {
  overview: 'M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z',
  backfill: 'M4 6h16 M4 12h16 M4 18h10 M17 16l3 3-3 3',
  records: 'M5 3h14v18H5z M8 7h8 M8 11h8 M8 15h5',
  failures: 'M12 3 2 21h20L12 3z M12 9v5 M12 17v1',
  simulations:
    'M9 3h6 M10 3v6l-6 9a2 2 0 0 0 2 3h12a2 2 0 0 0 2-3l-6-9V3 M7 15h10',
  configuration: 'M4 7h16 M4 17h16 M8 4v6 M16 14v6',
  refresh: 'M20 7v5h-5 M4 17v-5h5 M6 6a8 8 0 0 1 14 6 M4 12a8 8 0 0 0 14 6',
  arrow: 'M5 12h14 M13 6l6 6-6 6',
  lock: 'M6 10h12v11H6z M8 10V7a4 4 0 0 1 8 0v3',
  close: 'M6 6l12 12 M18 6 6 18',
  check: 'M5 12l4 4L19 6',
  menu: 'M4 6h16 M4 12h16 M4 18h16',
  search: 'M16 16l5 5 M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0',
};
@Component({
  selector: 'kit-icon',
  standalone: true,
  template: `<svg
    width="18"
    height="18"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    stroke-width="1.7"
    stroke-linecap="round"
    stroke-linejoin="round"
    aria-hidden="true"
    focusable="false"
  >
    <path [attr.d]="path()" />
  </svg>`,
})
export class Icon {
  readonly name = input('overview');
  readonly path = computed(() => paths[this.name()] ?? paths['overview']);
}
