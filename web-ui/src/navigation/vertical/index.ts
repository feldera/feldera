import { VerticalNavItemsType } from 'src/@core/layouts/types'

const navigation = (): VerticalNavItemsType => {
  return [
    {
      title: 'Home',
      path: '/home',
      icon: 'bx:home-circle'
    },
    {
      sectionTitle: 'Analytics'
    },
    {
      title: 'SQL Editor',
      path: '/analytics/editor',
      icon: 'bx:dock-top'
    },
    {
      title: 'Existing Programs',
      path: '/analytics/programs',
      icon: 'bx:list-ul'
    },
    {
      sectionTitle: 'Streaming'
    },
    {
      title: 'Pipeline Builder',
      path: '/streaming/builder',
      icon: 'gridicons:create'
    },
    {
      title: 'Pipeline Management',
      path: '/streaming/management',
      icon: 'bx:git-repo-forked'
    },
    //{
    //title: 'Introspection',
    //path: '/streaming/introspection',
    //icon: 'bx:chart'
    //},
    {
      sectionTitle: 'Input & Output'
    },
    {
      title: 'Connector Creator',
      path: '/data/create',
      icon: 'bx:coin-stack'
    },
    {
      title: 'Existing Connectors',
      path: '/data/list',
      icon: 'bx:download'
    }
    //{
    //title: 'Data Browser',
    //path: '/data/browser',
    //icon: 'bx:table'
    //}
  ]
}

export default navigation
