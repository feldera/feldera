import React, {type ReactNode} from 'react';
import clsx from 'clsx';
import BrowserOnly from '@docusaurus/BrowserOnly';

import CopyButton from '@theme/CodeBlock/Buttons/CopyButton';
import WordWrapButton from '@theme/CodeBlock/Buttons/WordWrapButton';
import type {Props} from '@theme/CodeBlock/Buttons';
import {useCodeBlockContext} from '@docusaurus/theme-common/internal';
import SandboxButton from "@site/src/theme/CodeBlock/SandboxButton";

import styles from './styles.module.css';

// Code block buttons are not server-rendered on purpose
// Adding them to the initial HTML is useless and expensive (due to JSX SVG)
// They are hidden by default and require React  to become interactive
export default function CodeBlockButtons({className, ...rest}: Props): ReactNode {
  const { metadata: { language, code } } = useCodeBlockContext()
  return (
    <BrowserOnly>
      {() => (
        <div className={clsx(className, styles.buttonGroup)}>
          {language === "sql" && <SandboxButton className={styles.codeButton} code={code} />}
          <WordWrapButton />
          <CopyButton />
        </div>
      )}
    </BrowserOnly>
  );
}
