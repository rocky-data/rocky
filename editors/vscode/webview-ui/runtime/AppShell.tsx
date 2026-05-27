import {
  Component,
  Suspense,
  useEffect,
  type ErrorInfo,
  type ReactNode,
} from "react";
import { getRpc } from "./rpcClient";
import { ThemeProvider } from "./ThemeProvider";

interface ErrorBoundaryState {
  error: Error | undefined;
}

class ErrorBoundary extends Component<{ children: ReactNode }, ErrorBoundaryState> {
  state: ErrorBoundaryState = { error: undefined };

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    console.error("Rocky webview crashed:", error, info.componentStack);
  }

  render(): ReactNode {
    if (this.state.error) {
      return (
        <div className="p-4 text-vscode-error">
          <p className="font-semibold">This panel hit an error.</p>
          <pre className="mt-2 whitespace-pre-wrap text-vscode-desc">
            {this.state.error.message}
          </pre>
        </div>
      );
    }
    return this.props.children;
  }
}

/**
 * Wraps a panel with theme context, an error boundary, and Suspense, and tells
 * the host the app has mounted (flushing buffered pushes) once on mount.
 */
export function AppShell({ children }: { children: ReactNode }) {
  useEffect(() => {
    getRpc().signalReady();
  }, []);

  return (
    <ThemeProvider>
      <ErrorBoundary>
        <Suspense fallback={<div className="p-4 text-vscode-desc">Loading…</div>}>
          {children}
        </Suspense>
      </ErrorBoundary>
    </ThemeProvider>
  );
}
