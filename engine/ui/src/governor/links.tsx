import type { MouseEvent } from "react";
import { Clip } from "../components";
import { auditPath, custodyPath, navigateTo } from "../router";

/**
 * A citation that leads to the custody screen for its subject. A link is a
 * navigation, never an action; the subject is the text it shows.
 */
export function CustodyLink({ subject, clip = false }: { subject: string; clip?: boolean }) {
  const href = custodyPath(subject);
  const onClick = (event: MouseEvent<HTMLAnchorElement>) => {
    event.preventDefault();
    navigateTo(href);
  };
  return (
    <a
      href={href}
      onClick={onClick}
      // Cut, the visible text is not the subject; the label keeps the whole
      // id as the link's name, so it is still announced and found by it.
      aria-label={clip ? subject : undefined}
      className="break-all text-sky-700 underline-offset-2 hover:underline dark:text-sky-400"
    >
      {clip ? <Clip value={subject} /> : subject}
    </a>
  );
}

/**
 * A citation that leads to the policy-decision ledger scoped to one product.
 *
 * Deliberately not a [`CustodyLink`]: custody resolves a model, a run or a plan
 * id, and `product:<name>` is none of those — the engine classifies it as a
 * model, finds nothing, and answers an empty chain (#2003). The scoped ledger
 * is what it can answer, so that is where this goes.
 */
export function AuditProductLink({ product, label }: { product: string; label?: string }) {
  const href = auditPath(product);
  const onClick = (event: MouseEvent<HTMLAnchorElement>) => {
    event.preventDefault();
    navigateTo(href);
  };
  return (
    <a
      href={href}
      onClick={onClick}
      className="break-all text-sky-700 underline-offset-2 hover:underline dark:text-sky-400"
    >
      {label ?? product}
    </a>
  );
}
