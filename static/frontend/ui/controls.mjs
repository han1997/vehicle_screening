import { mountFormPanel } from "./panels.mjs";
import { mountDateTime } from "./datetime.mjs";
import { mountCombobox, mountChoiceList } from "./choices.mjs";
import { mountFileInput } from "./files.mjs";

export class ControlHost {
  constructor({ overlays, referenceDate, onChoiceChange }) {
    this.overlays = overlays;
    this.referenceDate = referenceDate;
    this.onChoiceChange = onChoiceChange;
    this.controls = new Map();
  }
  mount(root) {
    const add = (selector, mount) =>
      root.querySelectorAll(selector).forEach((node) => {
        if (!this.controls.has(node)) this.controls.set(node, mount(node));
      });
    add("input[data-date-kind]", (input) =>
      mountDateTime(input, { overlays: this.overlays, referenceDate: this.referenceDate() })
    );
    add("input[list],select:not([data-native])", (input) =>
      mountCombobox(input, { overlays: this.overlays })
    );
    add("[data-choice-picker]", (picker) => mountChoiceList(picker, this.onChoiceChange));
    add("input[type=file]:not([hidden])", (input) => mountFileInput(input));
    add(".conditions-panel", mountFormPanel);
    this.update();
  }
  update() {
    for (const control of this.controls.values()) control.update?.();
  }
  destroy() {
    this.overlays.close("unmount", false);
    for (const control of this.controls.values()) control.destroy();
    this.controls.clear();
  }
}
