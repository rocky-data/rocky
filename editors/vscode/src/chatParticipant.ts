import * as path from "path";
import * as vscode from "vscode";
import { runAiExplain, runAiGenerate, runAiSync, runAiTest } from "./commands/ai";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type SlashCommand = "generate" | "explain" | "sync" | "test";

// ---------------------------------------------------------------------------
// Chat handler
// ---------------------------------------------------------------------------

async function handler(
  request: vscode.ChatRequest,
  _context: vscode.ChatContext,
  stream: vscode.ChatResponseStream,
  _token: vscode.CancellationToken,
): Promise<SlashCommand | undefined> {
  const cmd = request.command as SlashCommand | undefined;

  if (cmd === "generate") {
    return handleGenerate(request, stream);
  }
  if (cmd === "explain") {
    return handleExplain(stream);
  }
  if (cmd === "sync") {
    return handleSync(stream);
  }
  if (cmd === "test") {
    return handleTest(request, stream);
  }

  // Natural-language fallback
  await handleNaturalLanguage(request, stream);
  return undefined;
}

// ---------------------------------------------------------------------------
// Slash-command handlers
// ---------------------------------------------------------------------------

async function handleGenerate(
  request: vscode.ChatRequest,
  stream: vscode.ChatResponseStream,
): Promise<SlashCommand> {
  const intent = request.prompt.trim();
  if (!intent) {
    stream.markdown(
      "Please provide a description after `/generate`. Example:\n\n`@rocky /generate monthly revenue per customer from the orders table`",
    );
    return "generate";
  }

  stream.markdown(`Generating Rocky model for: _${intent}_\n\n`);
  try {
    const source = await runAiGenerate(intent);
    stream.markdown("```rocky\n" + source + "\n```\n\n");
    stream.markdown(
      "_Model generated. Opening in editor — review and save when ready._",
    );
    // Also open in the editor so the user can save it.
    const doc = await vscode.workspace.openTextDocument({
      content: source,
      language: "rocky",
    });
    await vscode.window.showTextDocument(doc, { preserveFocus: true });
  } catch (err) {
    stream.markdown(
      `**Error generating model:** ${errorMessage(err)}\n\nCheck the Rocky output channel for details.`,
    );
  }
  return "generate";
}

async function handleExplain(
  stream: vscode.ChatResponseStream,
): Promise<SlashCommand> {
  const modelName = activeModelName();
  if (modelName) {
    stream.markdown(`Explaining model: _${modelName}_\n\n`);
  } else {
    stream.markdown("Explaining all models with intent docs…\n\n");
  }

  try {
    const output = await runAiExplain(modelName);
    stream.markdown(output.trim() || "_Intent generated and saved to model files._");
  } catch (err) {
    stream.markdown(
      `**Error explaining model:** ${errorMessage(err)}\n\nCheck the Rocky output channel for details.`,
    );
  }
  return "explain";
}

async function handleSync(
  stream: vscode.ChatResponseStream,
): Promise<SlashCommand> {
  stream.markdown("Detecting schema changes and proposing sync updates…\n\n");

  try {
    const output = await runAiSync();
    stream.markdown(output.trim() || "_No models need syncing._");
  } catch (err) {
    stream.markdown(
      `**Error syncing models:** ${errorMessage(err)}\n\nCheck the Rocky output channel for details.`,
    );
  }
  return "sync";
}

async function handleTest(
  request: vscode.ChatRequest,
  stream: vscode.ChatResponseStream,
): Promise<SlashCommand> {
  // Prefer explicit arg; fall back to active editor; then all models.
  const argModel = request.prompt.trim() || undefined;
  const modelName = argModel ?? activeModelName();

  if (modelName) {
    stream.markdown(`Generating quality tests for: _${modelName}_\n\n`);
  } else {
    stream.markdown("Generating quality tests for all models…\n\n");
  }

  try {
    const output = await runAiTest(modelName);
    stream.markdown(output.trim() || "_Tests generated and saved to model files._");
  } catch (err) {
    stream.markdown(
      `**Error generating tests:** ${errorMessage(err)}\n\nCheck the Rocky output channel for details.`,
    );
  }
  return "test";
}

// ---------------------------------------------------------------------------
// Natural-language fallback
// ---------------------------------------------------------------------------

async function handleNaturalLanguage(
  request: vscode.ChatRequest,
  stream: vscode.ChatResponseStream,
): Promise<void> {
  const models = await vscode.lm.selectChatModels({ vendor: "copilot" });
  const model = models[0];

  if (!model) {
    stream.markdown(availableCommandsMarkdown());
    return;
  }

  const systemContext = [
    "You are Rocky, an AI assistant for the Rocky SQL transformation engine.",
    "Rocky is a typed-program layer above the warehouse with branches, replay, column-level lineage, compile-time type safety, and per-model cost attribution.",
    "Models are written in either the `.rocky` DSL or plain `.sql` files.",
    "",
    "You have four available actions the user can invoke as slash commands:",
    "- `/generate <description>` — generate a Rocky model from a natural-language description.",
    "- `/explain` — generate intent documentation for the active model.",
    "- `/sync` — detect source schema changes and propose model sync updates.",
    "- `/test <model>` — generate quality test cases for a named model.",
    "",
    "Answer the user's question directly when possible.",
    "If the user's intent maps cleanly to one of the four actions, suggest the appropriate slash command.",
    "Keep answers concise and actionable.",
  ].join("\n");

  const messages = [
    vscode.LanguageModelChatMessage.User(systemContext),
    vscode.LanguageModelChatMessage.User(request.prompt),
  ];

  try {
    const response = await model.sendRequest(
      messages,
      {},
    );
    for await (const chunk of response.text) {
      stream.markdown(chunk);
    }
  } catch {
    // If the LLM call fails for any reason, fall back to command listing.
    stream.markdown(availableCommandsMarkdown());
  }
}

// ---------------------------------------------------------------------------
// Followup provider
// ---------------------------------------------------------------------------

function followupProvider(
  result: vscode.ChatResult,
): vscode.ChatFollowup[] | undefined {
  const lastCommand = result.metadata?.lastCommand as SlashCommand | undefined;

  if (lastCommand === "generate") {
    return [
      {
        prompt: "/explain",
        label: "Explain the generated model",
      },
      {
        prompt: "/test",
        label: "Generate quality tests for the model",
      },
    ];
  }
  if (lastCommand === "explain") {
    return [
      {
        prompt: "/test",
        label: "Generate quality tests",
      },
    ];
  }
  if (lastCommand === "sync") {
    return [
      {
        prompt: "/explain",
        label: "Explain the newly synced sources",
      },
    ];
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

export function registerChatParticipant(
  context: vscode.ExtensionContext,
): void {
  const participant = vscode.chat.createChatParticipant(
    "rocky-data.rocky",
    async (request, ctx, stream, token) => {
      const lastCommand = await handler(request, ctx, stream, token);
      return { metadata: { lastCommand } };
    },
  );

  participant.iconPath = vscode.Uri.joinPath(
    context.extensionUri,
    "icons",
    "rocky-icon-128.png",
  );

  participant.followupProvider = {
    provideFollowups(
      result: vscode.ChatResult,
      _context: vscode.ChatContext,
      _token: vscode.CancellationToken,
    ): vscode.ChatFollowup[] {
      return followupProvider(result) ?? [];
    },
  };

  context.subscriptions.push(participant);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function activeModelName(): string | undefined {
  const editor = vscode.window.activeTextEditor;
  if (!editor) return undefined;
  return path
    .basename(editor.document.fileName)
    .replace(/\.(rocky|sql)$/, "");
}

function errorMessage(err: unknown): string {
  return (err as Error)?.message ?? String(err);
}

function availableCommandsMarkdown(): string {
  return [
    "I can help with your Rocky pipelines. Available commands:\n",
    "| Command | What it does |",
    "|---------|-------------|",
    "| `/generate <description>` | Generate a Rocky model from a natural-language description |",
    "| `/explain` | Generate intent documentation for the active model |",
    "| `/sync` | Detect source schema changes and propose model updates |",
    "| `/test <model>` | Generate quality test cases for a model |",
    "",
    "_GitHub Copilot is not available — slash commands above route directly to the Rocky CLI._",
  ].join("\n");
}
