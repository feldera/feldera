import { redirect } from 'next/navigation'

export default async function Root() {
  redirect('/home')
}
