import type { MouseEvent } from "react";
import { custodyPath, navigateTo } from "../router";

/**
 * A citation that leads to the custody screen for its subject. A link is a
 * navigation, never an action; the subject is the text it shows.
 */
export function CustodyLink({ subject }: { subject: string }) {
  const href = custodyPath(subject);
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
      {subject}
    </a>
  );
}
