# Thinking Guides

These guides are short review prompts for the vehicle-screening application. Concrete signatures,
payload rules and implementation examples belong in the backend/frontend specs linked below.

## Guide Index

| Guide                                                   | Use when                                                                    |
| ------------------------------------------------------- | --------------------------------------------------------------------------- |
| [Cross-Layer Thinking](./cross-layer-thinking-guide.md) | A field or behavior crosses form, API, storage, result or export boundaries |
| [Code Reuse Thinking](./code-reuse-thinking-guide.md)   | Changing a default/constant, adding a helper/control or moving modules      |

## Before Editing

- Read the [backend index](../backend/index.md) and/or [frontend index](../frontend/index.md) for
  the layer being changed, then follow its pre-development checklist.
- Search for a field, value or helper before modifying it. Defaults and mode fields appear in
  several runtimes; finding one occurrence is not proof that the change is complete.
- Use [Screening Contracts](../backend/screening-contracts.md) for actual mode/snapshot/time/API
  behavior, not archived migration notes or a generic framework assumption.
- Keep real traffic files and people libraries out of research, fixtures, logs and screenshots.
  Use the existing synthetic test support instead.

## After Editing

- Follow the relevant index's quality check, including the full regression suite for cross-layer
  behavior. Report missing manual/Win7 coverage separately from automated passes.
- Capture newly learned executable rules in the appropriate spec file, not only a chat summary.
