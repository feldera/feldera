// Navigates to /home on load.
import { useEffect } from 'react'
import { useRouter } from 'next/router'
import LoadingScreen from '$lib/components/common/spinner'

const Home = () => {
  const router = useRouter()
  useEffect(() => {
    router.replace('/home')
  }, [router])

  return <LoadingScreen sx={{ height: '100%' }} />
}

export default Home
