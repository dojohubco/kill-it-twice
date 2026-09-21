import { Component, input } from '@angular/core';
import { label, tone } from '../core/value';
@Component({
  selector: 'kit-badge',
  standalone: true,
  template: `<span class="badge" [class]="'badge ' + tone(state())"
    ><span class="status-dot" aria-hidden="true"></span
    >{{ label(state()) }}</span
  >`,
})
export class Badge {
  readonly state = input<unknown>('unknown');
  readonly label = label;
  readonly tone = tone;
}
