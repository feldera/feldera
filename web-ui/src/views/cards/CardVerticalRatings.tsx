// ** MUI Imports
import Box from '@mui/material/Box'
import Card from '@mui/material/Card'
import Rating from '@mui/material/Rating'
import Button from '@mui/material/Button'
import Typography from '@mui/material/Typography'
import CardHeader from '@mui/material/CardHeader'
import CardContent from '@mui/material/CardContent'
import CardActions from '@mui/material/CardActions'

const CardVerticalRatings = () => {
  return (
    <Card>
      <CardHeader title='The Best Answers' />
      <CardContent>
        <Box sx={{ mb: 5, display: 'flex', flexWrap: 'wrap', alignItems: 'center' }}>
          <Rating readOnly value={5} name='read-only' sx={{ marginRight: 2 }} />
          <Typography variant='body2'>5 Star | 98 reviews</Typography>
        </Box>
        <Typography variant='body2' sx={{ marginBottom: 3.25 }}>
          If you are looking for a new way to promote your business that won’t cost you more money, maybe printing is
          one of the options you won’t resist.
        </Typography>
        <Typography variant='body2'>
          Printing is a widely use process in making printed materials that are used for advertising. It become fast,
          easy and simple. If you want your promotional material to be an eye-catching.
        </Typography>
      </CardContent>
      <CardActions className='card-action-dense'>
        <Button>Location</Button>
        <Button>Reviews</Button>
      </CardActions>
    </Card>
  )
}

export default CardVerticalRatings
