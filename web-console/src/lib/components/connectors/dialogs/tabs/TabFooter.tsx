// Footer for a dialog with tabs it contains the next and previous buttons. The

// next button can turn into a submit button if we're on the last tab.
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'

export const TabFooter = ({
  activeTab,
  setActiveTab,
  tabs,
  submitButton
}: {
  activeTab: string
  setActiveTab: any
  tabs: readonly string[]
  submitButton: JSX.Element
}) => {
  const prevTab = tabs[tabs.indexOf(activeTab) - 1]
  const nextTab = tabs[tabs.indexOf(activeTab) + 1]
  const onLastTab = activeTab === tabs[tabs.length - 1]
  const onFirstTab = activeTab === tabs[0]

  const nextOrSaveButton = onLastTab ? (
    submitButton
  ) : (
    <Button
      variant='contained'
      color='primary'
      endIcon={<i className={`bx bx-right-arrow-alt`} style={{}} />}
      onClick={() => {
        setActiveTab(nextTab)
      }}
      data-testid='button-next'
    >
      Next
    </Button>
  )

  return (
    <Box sx={{ display: 'flex', width: '100%', mt: 'auto', pt: 4, justifyContent: 'space-between' }}>
      <Button
        variant='outlined'
        color='secondary'
        disabled={onFirstTab}
        onClick={() => setActiveTab(prevTab)}
        startIcon={<i className={`bx bx-left-arrow-alt`} style={{}} />}
        data-testid='button-previous'
      >
        Previous
      </Button>
      {nextOrSaveButton}
    </Box>
  )
}
