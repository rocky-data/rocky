# Rocky documentation style

Write so that a competent engineer who has never used Rocky can follow the page
on the first read. The standard below is based on ASD-STE100 (Simplified
Technical English), the writing standard used for aircraft maintenance manuals.

Apply this to every public page: `README.md`, `ROCKY_EXPLAINED.md`, and
everything under `docs/src/content/docs/`.

## The rules

**Sentences**

- One idea per sentence. Maximum 25 words. Aim for 15.
- Use the active voice. Say who does what.
- Use the simple present tense where you can.
- Start a procedure step with the verb: "Run `rocky plan`." Not "The plan
  command can then be run."

```
NO   The plan is written to the state store, from where it can subsequently
     be approved by a reviewer before it is applied.        (24 words, passive)

YES  Rocky writes the plan to the state store.
     A reviewer approves it. Then you apply it.             (3 sentences, active)
```

**Words**

- One word, one meaning. Pick a term and keep it. Do not use "job", "task" and
  "run" for the same thing on one page.
- Explain a term of art the first time it appears, then link the glossary:
  "a watermark (the timestamp of the newest row Rocky has already loaded)".
- Prefer the short word: "use" not "utilise", "start" not "initiate", "about"
  not "approximately".
- Do not drop the article. Write "the plan", not "plan".
- Write what a thing does, not how good it is. Cut "powerful", "seamless",
  "battle-tested", "first-class".

**Paragraphs**

- Maximum 6 sentences. Break a longer one.
- One topic per paragraph. Put the point in the first sentence.
- Give every section an opening sentence that says what the section is for.

**Headings**

- Name the mechanism: "How Rocky advances the watermark". Not "How it works".
- The reader scans headings to find one task. Write them for that reader.

## Diagrams

Draw the mechanism when the text describes a flow, a tree, a lifecycle, or a
sequence of states. A picture replaces prose — delete the paragraphs it makes
redundant, do not keep both.

Use ASCII. It renders everywhere, it diffs cleanly in git, and it needs no
build step.

```
                  ┌──────────┐
   your SQL ─────►│ compile  │─────► typed IR ─────► dialect SQL
                  └──────────┘                            │
                        │                                 ▼
                        │ errors                     ┌──────────┐
                        └───────────────────────────►│ warehouse│
                          E001 … E036                └──────────┘
```

Rules for a diagram:

- It must show something the sentence next to it does not already say.
- Label every box and arrow. An unlabelled arrow teaches nothing.
- Keep it under 80 columns so it does not wrap on a narrow screen.
- Put the happy path left to right, or top to bottom. Keep error paths to one
  side.

## What not to change

- Code, commands, flags, file paths, configuration keys, and error codes are
  exact. Copy them character for character.
- Do not change a heading that another page links to as an anchor. Search for
  `#the-heading-slug` before you rename a heading.
- Do not "simplify" a technical fact into a wrong one. If the plain wording
  loses a condition, keep the condition and use two sentences.
- Keep the `Flags` / `Examples` / `Related commands` structure on reference
  pages. A lookup page is a table, not an essay.
- Em dashes stay in headings, labels, and code. Remove them from body prose
  where a full stop works better.

## Before you commit

- Read the page aloud. If you run out of breath, the sentence is too long.
- Check every command and flag you mention against the code.
- Check that each diagram matches the text beside it.
