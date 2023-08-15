import { Theme } from '@mui/material/styles'

const Card = (theme: Theme) => {
  return {
    MuiCard: {
      styleOverrides: {
        root: {
          boxShadow: theme.shadows[6],
          '& .card-more-options': {
            marginTop: theme.spacing(-1),
            marginRight: theme.spacing(-3)
          },
          '& .MuiTableContainer-root, & .MuiDataGrid-root, & .MuiDataGrid-columnHeaders': {
            borderRadius: 0
          }
        }
      },
      defaultProps: {
        elevation: 6
      }
    },
    MuiCardHeader: {
      styleOverrides: {
        root: {
          padding: theme.spacing(6),
          '& + .MuiCardContent-root, & + .MuiCardActions-root, & + .MuiCollapse-root .MuiCardContent-root': {
            paddingTop: 0
          },
          '& .MuiCardHeader-subheader': {
            fontSize: '0.875rem',
            marginTop: theme.spacing(1.25),
            color: theme.palette.text.secondary
          }
        },
        title: {
          lineHeight: 1.6,
          fontWeight: 500,
          fontSize: '1.25rem',
          letterSpacing: '0.0125em',
          '@media (min-width: 600px)': {
            fontSize: '1.25rem'
          }
        },
        action: {
          marginTop: 0,
          marginRight: 0
        }
      }
    },
    MuiCardContent: {
      styleOverrides: {
        root: {
          padding: theme.spacing(6),
          '& + .MuiCardHeader-root, & + .MuiCardContent-root, & + .MuiCardActions-root': {
            paddingTop: 0
          },
          '&:last-of-type': {
            paddingBottom: theme.spacing(6)
          }
        }
      }
    },
    MuiCardActions: {
      styleOverrides: {
        root: {
          padding: theme.spacing(5),
          '&.card-action-dense': {
            padding: theme.spacing(0, 2.5, 2.5),
            '.MuiCard-root .MuiCardMedia-root + &': {
              paddingTop: theme.spacing(2.5)
            },
            '.MuiCard-root &:first-of-type': {
              paddingTop: theme.spacing(5),
              paddingBottom: theme.spacing(5)
            }
          },
          '& .MuiButton-text': {
            paddingLeft: theme.spacing(2.5),
            paddingRight: theme.spacing(2.5)
          }
        }
      }
    }
  }
}

export default Card
