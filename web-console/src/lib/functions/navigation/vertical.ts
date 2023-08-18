// The main menu that shows up on the left side of the screen.
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
    // {
    //   title: 'SQL Editor',
    //   path: '/analytics/editor',
    //   icon: 'bx:dock-top'
    // },
    {
      title: 'SQL Programs',
      path: ['/analytics/programs', '/analytics/editor'],
      icon: 'bx:align-left' // 'bx:list-ul'
    },
    {
      sectionTitle: 'Streaming'
    },
    // {
    //   title: 'Pipeline Builder',
    //   path: '/streaming/builder',
    //   icon: 'gridicons:create'
    // },
    {
      title: 'Pipelines', // 'Pipeline Management',
      path: ['/streaming/management', '/streaming/builder'],
      icon: 'bx:git-repo-forked'
    },
    //{
    //title: 'Data Inspection',
    //path: '/streaming/inspection',
    //icon: 'bx:chart'
    //},
    {
      sectionTitle: 'Input & Output'
    },
    // {
    //   title: 'Connector Creator',
    //   path: '/connectors/create',
    //   icon: 'bx:coin-stack'
    // },
    {
      title: 'Connectors', // 'Existing Connectors',
      path: ['/connectors/list', '/connectors/create'],
      icon: 'bx:unite' // 'bx:download'
    },
    //{
    //title: 'Data Browser',
    //path: '/connectors/browser',
    //icon: 'bx:table'
    //}
    {
      sectionTitle: 'Ecosystem'
    },
    {
      title: 'Documentation',
      path: 'https://docs.feldera.io/docs/sql/intro',
      icon: 'bx:file',
      openInNewTab: true
    }
  ]
}

export default navigation
