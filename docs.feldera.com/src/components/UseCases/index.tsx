import React, {type ReactNode} from "react";
import Link from "@docusaurus/Link";
import styles from "./styles.module.css";

// One card in a use case grid: a docs guide, a runnable demo on GitHub, or a reference page.
export type UseCaseItem = {
  title: string;
  description: string;
  href: string;
  kind: "Guide" | "Demo" | "Reference" | "Customer story";
};

export function UseCaseGrid({items}: {items: UseCaseItem[]}): ReactNode {
  return (
    <div className={styles.grid}>
      {items.map((item) => (
        <Link key={item.href} to={item.href} className={`card ${styles.card}`}>
          <span className={styles.kind}>
            {item.kind}
            {item.kind === "Demo" && <span aria-hidden="true"> · GitHub ↗</span>}
            {item.kind === "Customer story" && <span aria-hidden="true"> ↗</span>}
          </span>
          <span className={styles.title}>{item.title}</span>
          <span className={styles.description}>{item.description}</span>
        </Link>
      ))}
    </div>
  );
}

// A large card that opens one use case hub.
export type HubItem = {
  title: string;
  description: string;
  href: string;
  includes: string[];
};

// The three hubs, defined once: the docs home and the Guides page show them.
export const HUBS: HubItem[] = [
  {
    title: "Agents",
    description: "Give agents SQL views over all of your data, historical and live. Agents write the SQL, subscribe to the changes and act on them.",
    href: "/use_cases/agents",
    includes: ["Instantly fresh context: SQL views that instantly reflect changes to live and historical data", "Agent security: permission checks for every user and agent"],
  },
  {
    title: "Real-time pipelines",
    description: "Replace scheduled batch jobs with SQL pipelines that update when the input changes.",
    href: "/use_cases/real-time-pipelines",
    includes: ["Batch jobs to incremental pipelines", "Real-time medallion architecture", "CDC with Debezium"],
  },
  {
    title: "Fraud detection",
    description: "Compute fraud features in SQL. Use the same queries to train on history and to score live transactions.",
    href: "/use_cases/fraud-detection",
    includes: ["Real-time feature engineering", "An ML model in the loop", "Hopsworks feature store"],
  },
];

export function HubGrid({items = HUBS}: {items?: HubItem[]}): ReactNode {
  return (
    <div className={styles.hubGrid}>
      {items.map((item) => (
        <Link key={item.href} to={item.href} className={`card ${styles.card} ${styles.hub}`}>
          {/* A no-break space keeps the arrow on the same line as the last word. */}
          <span className={styles.hubTitle}>{item.title}{" →"}</span>
          <span className={styles.description}>{item.description}</span>
          <ul className={styles.includes}>
            {item.includes.map((line) => (
              <li key={line}>{line}</li>
            ))}
          </ul>
        </Link>
      ))}
    </div>
  );
}

// A one-row line drawing: boxes joined by labeled arrows, with an optional arrow back from the
// last box to box `loopTo` (for loops such as an agent that changes the views it watches).
export type FlowNode = {label: string; sub?: string};

export function FlowDiagram({
  nodes,
  edges,
  loop,
  loopTo = 0,
  caption,
}: {
  nodes: FlowNode[];
  edges: string[];
  loop?: string;
  loopTo?: number;
  caption?: string;
}): ReactNode {
  // Size boxes and gaps from the text, so labels never overflow. Approximate advance widths:
  // DM Sans 15px about 8.4px per character, Geist Mono 11px about 6.7px per character.
  const sans = (t: string) => t.length * 8.4;
  const mono = (t: string) => t.length * 6.7;
  const boxW = Math.ceil(Math.max(140, ...nodes.map((n) => Math.max(sans(n.label), mono(n.sub ?? "")) + 28)));
  const boxH = 64;
  const gap = Math.ceil(Math.max(88, ...edges.map((e) => mono(e) + 32)));
  const top = 12;
  const width = nodes.length * boxW + (nodes.length - 1) * gap + 2;
  const height = top + boxH + (loop ? 56 : 12);
  const x = (i: number) => 1 + i * (boxW + gap);
  return (
    <figure className={styles.figure}>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        className={styles.diagram}
        role="img"
        aria-label={caption ?? nodes.map((n) => n.label).join(" to ")}
      >
        <defs>
          <marker id="uc-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="8" markerHeight="8" orient="auto">
            <path d="M0 0 L8 4 L0 8" fill="none" stroke="currentColor" strokeWidth="1.2" />
          </marker>
        </defs>
        {nodes.map((n, i) => (
          <g key={n.label}>
            <rect x={x(i)} y={top} width={boxW} height={boxH} className={styles.box} />
            <text x={x(i) + boxW / 2} y={top + (n.sub ? 28 : 37)} className={styles.boxLabel}>
              {n.label}
            </text>
            {n.sub && (
              <text x={x(i) + boxW / 2} y={top + 47} className={styles.boxSub}>
                {n.sub}
              </text>
            )}
          </g>
        ))}
        {edges.map((label, i) => {
          const x1 = x(i) + boxW;
          const x2 = x(i + 1) - 2;
          const y = top + boxH / 2;
          return (
            <g key={`${label}-${i}`}>
              <line x1={x1} y1={y} x2={x2} y2={y} className={styles.edge} markerEnd="url(#uc-arrow)" />
              <text x={(x1 + x2) / 2} y={y - 8} className={styles.edgeLabel}>
                {label}
              </text>
            </g>
          );
        })}
        {loop && (
          <g>
            <path
              d={`M ${x(nodes.length - 1) + boxW / 2} ${top + boxH} V ${top + boxH + 32} H ${x(loopTo) + boxW / 2} V ${top + boxH + 4}`}
              className={styles.edge}
              markerEnd="url(#uc-arrow)"
            />
            <text x={(x(loopTo) + x(nodes.length - 1) + boxW) / 2} y={top + boxH + 50} className={styles.edgeLabel}>
              {loop}
            </text>
          </g>
        )}
      </svg>
      {caption && <figcaption className={styles.caption}>{caption}</figcaption>}
    </figure>
  );
}
