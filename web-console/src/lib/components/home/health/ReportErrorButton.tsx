import newGithubIssueUrl from 'new-github-issue-url'
import { match, P } from 'ts-pattern'

import { Button } from '@mui/material'

type ReportDetails = Record<string, string | { url: string } | (() => Promise<string>)>

const detailString = async (detail: ReportDetails[string]) =>
  match(detail)
    .with(P.string, str => Promise.resolve(str))
    .with({ url: P.select() }, url => fetch(url).then(r => r.text()))
    .with(P.instanceOf(Function).select(), f => f())
    .exhaustive()

const sendReport = async (report: ReportDetails) => {
  const url = newGithubIssueUrl({
    user: 'feldera',
    repo: 'feldera',
    title: 'User report: system error',
    body: (
      await Promise.all(Object.entries(report).map(async ([key, detail]) => key + ':\n' + (await detailString(detail))))
    ).join('\n')
  })
  window.open(url, '_blank', 'noreferrer')
}

export const ReportErrorButton = (props: { report: ReportDetails }) => {
  return (
    <Button
      variant='contained'
      size='small'
      color='success'
      sx={{ position: 'absolute', top: 0, right: 0 }}
      onClick={() => sendReport(props.report)}
    >
      report
    </Button>
  )
}
