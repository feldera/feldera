import newGithubIssueUrl from 'new-github-issue-url'
import sortOn from 'sort-on'

const sections = {
  '1-description': '\n**Describe the bug**\n',
  '2-repro': '\n**To Reproduce**\n',
  '3-expected': '\n**Expected behavior**\n',
  '4-screenshots': '\n**Screenshots**\n',
  '5-context': '\n**Context:**\n',
  '6-extra': '\n**Additional context**\n'
}

export type ReportDetails = Record<'name' | keyof typeof sections, string>

export const defaultGithubReportSections: Partial<Record<keyof typeof sections, string>> = {
  '1-description': '<!-- A clear and concise description of what the bug is. -->',
  '2-repro': `<!-- Steps to reproduce the behavior e.g., Curl commands or steps in the UI such as:
1. Go to '...'
2. Click on '....'
3. Scroll down to '....'
4. See error -->`,
  '3-expected': '<!-- A clear and concise description of what you expected to happen. -->',
  '4-screenshots': '<!-- If applicable, add screenshots/logs to help explain your problem. -->',
  '5-context': `
 - Feldera Version: <!-- [e.g. 0.12.0] -->
 - Environment: <!-- e.g. Docker, Cloud Sandbox, Native -->
 - Browser: <!-- chrome, safari -->
`,
  '6-extra': 'Add any other context about the problem here.'
}

export const sendGithubReport = async ({ name, ...report }: ReportDetails) => {
  const url = newGithubIssueUrl({
    user: 'feldera',
    repo: 'feldera',
    title: name ? name : 'User report: system error',
    body: (
      await Promise.all(
        sortOn(Object.entries(report), (o) => o[0]).map(
          async ([key, detail]) => sections[key as unknown as keyof typeof report] + detail
        )
      )
    ).join('\n'),
    template: 'bug_report.md'
  })
  window.open(url, '_blank', 'noreferrer')
}
