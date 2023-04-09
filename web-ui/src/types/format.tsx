// ** Types

export type UsersType = {
  id: number
  role: string
  email: string
  status: string
  avatar: string
  billing: string
  company: string
  country: string
  contact: string
  fullName: string
  username: string
  currentPlan: string
}

export type ProjectListDataType = {
  id: number
  img: string
  hours: string
  totalTask: string
  projectType: string
  projectTitle: string
  progressValue: number
}
