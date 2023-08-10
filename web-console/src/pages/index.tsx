import LoadingScreen from '$lib/components/common/spinner'
import { useRouter } from 'next/router'
// Navigates to /home on load.
import { useEffect } from 'react'

const Home = () => {
  const router = useRouter()
  useEffect(() => {
    router.replace('/home')
  }, [router])

  return <LoadingScreen sx={{ height: '100%' }} />
}

export default Home
