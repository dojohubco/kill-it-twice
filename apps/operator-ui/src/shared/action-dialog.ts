import { dialogKeyboard } from './dialog-keyboard';
import {
  Component,
  ElementRef,
  effect,
  inject,
  viewChild,
} from '@angular/core';
import { Actions } from '../core/actions';
import { Icon } from './icon';
@Component({
  selector: 'kit-action-dialog',
  standalone: true,
  imports: [Icon],
  templateUrl: './action-dialog.html',
})
export class ActionDialog {
  readonly dialogKeyboard = dialogKeyboard;
  readonly actions = inject(Actions);
  readonly dialog = viewChild<ElementRef<HTMLDialogElement>>('dialog');
  constructor() {
    effect(() => {
      const el = this.dialog()?.nativeElement;
      if (!el) return;
      if (this.actions.current() && !el.open) el.showModal();
      else if (!this.actions.current() && el.open) el.close();
    });
  }
  cancel(event: Event): void {
    event.preventDefault();
    this.actions.close();
  }
  backdrop(event: MouseEvent): void {
    if (event.target === this.dialog()?.nativeElement) this.actions.close();
  }
}
