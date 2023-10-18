// The main menu that shows up on the left side of the screen.
import { VerticalNavItemsType } from 'src/@core/layouts/types'
import IconAlignLeft from '~icons/bx/align-left'
import IconFile from '~icons/bx/file'
import IconGitRepoForked from '~icons/bx/git-repo-forked'
import IconHomeCircle from '~icons/bx/home-circle'
import IconUnite from '~icons/bx/unite'

const navigation = (): VerticalNavItemsType => {
  return [
    {
      title: 'Home',
      path: '/home',
      icon: IconHomeCircle
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
      icon: IconAlignLeft
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
      icon: IconGitRepoForked
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
      icon: IconUnite
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
      path: 'https://www.feldera.com/docs/',
      icon: IconFile,
      openInNewTab: true
    }
  ]
}

export default navigation
