import { OpenAPI } from '$lib/services/manager'
// The main menu that shows up on the left side of the screen.
import { VerticalNavItemsType } from 'src/@core/layouts/types'
import IconAlignLeft from '~icons/bx/align-left'
import IconCodeCurly from '~icons/bx/code-curly'
import IconEnvelope from '~icons/bx/envelope'
import IconFile from '~icons/bx/file'
import IconGitRepoForked from '~icons/bx/git-repo-forked'
import IconHomeCircle from '~icons/bx/home-circle'
import IconRocket from '~icons/bx/rocket'
import IconServer from '~icons/bx/server'
import IconUnite from '~icons/bx/unite'
import IconSlack from '~icons/bxl/slack'
import CogOutline from '~icons/mdi/cog-outline'

const navigation = (): VerticalNavItemsType => {
  return [
    {
      title: 'Home',
      path: '/home',
      icon: IconHomeCircle,
      testid: 'button-vertical-nav-home'
    },
    {
      title: 'Demos',
      path: '/demos',
      icon: IconRocket
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
      icon: IconAlignLeft,
      testid: 'button-vertical-nav-sql-programs'
    },
    // {
    //   title: 'Pipeline Builder',
    //   path: '/streaming/builder',
    //   icon: 'gridicons:create'
    // },
    {
      title: 'Pipelines', // 'Pipeline Management',
      path: ['/streaming/management', '/streaming/builder'],
      icon: IconGitRepoForked,
      testid: 'button-vertical-nav-pipelines'
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
      title: 'Services',
      path: ['/services/list', '/services/create'],
      icon: IconServer,
      testid: 'button-vertical-nav-services'
    },
    {
      title: 'Connectors',
      path: ['/connectors/list', '/connectors/create'],
      icon: IconUnite,
      testid: 'button-vertical-nav-connectors'
    },
    //{
    //title: 'Data Browser',
    //path: '/connectors/browser',
    //icon: 'bx:table'
    //}
    {
      sectionTitle: 'Platform'
    },
    {
      title: 'Settings',
      path: '/settings',
      icon: CogOutline
    },
    {
      title: 'Documentation',
      path: 'https://www.feldera.com/docs/',
      icon: IconFile,
      openInNewTab: true,
      testid: 'button-vertical-nav-documentation'
    },
    {
      title: 'Swagger',
      path: OpenAPI.BASE + '/swagger-ui/',
      icon: IconCodeCurly,
      openInNewTab: true,
      testid: 'button-vertical-nav-swagger'
    },
    {
      title: 'Email',
      path: 'mailto:learnmore@feldera.com',
      icon: IconEnvelope,
      openInNewTab: true,
      testid: 'button-vertical-nav-email'
    },
    {
      title: 'Slack',
      path: 'https://felderacommunity.slack.com',
      icon: IconSlack,
      openInNewTab: true,
      testid: 'button-vertical-nav-slack'
    }
  ]
}

export default navigation
