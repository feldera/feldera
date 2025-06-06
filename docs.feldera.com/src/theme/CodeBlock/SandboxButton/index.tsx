import React from 'react';
import clsx from 'clsx';
import {translate} from '@docusaurus/Translate';
import type {Props} from '@theme/CodeBlock/CopyButton';

import styles from './styles.module.css';

export default function SandboxButton({code, className}: Props): JSX.Element {
  // generate a random name
  const randomName = 'docs-' + Math.random().toString(36).substring(7);
  const params = '?name=' + randomName + '&code=' + encodeURIComponent(code);

  let domain = 'https://try.feldera.com/create/';
  if (process.env.NODE_ENV === 'development') {
    domain = 'http://localhost:8080/create/';
  }
  const href = domain + params;
console.log('styles', styles)
  return (
    <button>
      <a
        href={href}
        target="_blank"
        rel="noopener noreferrer"
        aria-label={translate({
          id: 'theme.CodeBlock.SandboxButtonAriaLabel',
          message: 'Run code in the Feldera Online Sandbox',
          description: 'The ARIA label for run in sandbox code blocks button',
        })}
        title={translate({
          id: 'theme.CodeBlock.runInSandbox',
          message: 'Run in the Sandbox',
          description: 'The run button label on code blocks',
        })}
        className={clsx('clean-btn', className, styles.sandboxButton)}
      >
        Run
      </a>
    </button>
  );
}