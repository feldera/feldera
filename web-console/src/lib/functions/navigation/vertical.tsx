import { OpenAPI } from '$lib/services/manager'
import SalesforceSlack from '$public/icons/vendors/salesforce-slack.svg'

// The main menu that shows up on the left side of the screen.
import { VerticalNavItemsType } from '@core/layouts/types'

// TODO: remove conditional Settings page link display
// after more content is added to Settings page
// so it is never empty
const navigation = (props: { showSettings: boolean }): VerticalNavItemsType => {
  return [
    {
      title: 'Home',
      path: '/home',
      icon: <i className='bx bx-home-circle' />,
      testid: 'button-vertical-nav-home'
    },
    {
      title: 'Demos',
      path: '/demos',
      icon: <i className='bx bx-rocket' />
    },
    {
      sectionTitle: 'Analytics'
    },
    {
      title: 'SQL Programs',
      path: ['/analytics/programs', '/analytics/editor'],
      icon: <i className='bx bx-align-left' />,
      testid: 'button-vertical-nav-sql-programs'
    },
    {
      title: 'Pipelines',
      path: ['/streaming/management', '/streaming/builder'],
      icon: <i className='bx bx-git-repo-forked' />,
      testid: 'button-vertical-nav-pipelines'
    },
    {
      sectionTitle: 'Input & Output'
    },
    {
      title: 'Connectors',
      path: ['/connectors/list', '/connectors/create'],
      icon: <i className='bx bx-unite' />,
      testid: 'button-vertical-nav-connectors'
    },
    {
      sectionTitle: 'Platform'
    },
    [
      props.showSettings
        ? [
            {
              title: 'Settings',
              path: '/settings',
              icon: <i className='bx bx-cog' />
            }
          ]
        : []
    ],
    {
      title: 'Documentation',
      path: 'https://www.feldera.com/docs/',
      icon: <i className='bx bx-file' />,
      openInNewTab: true,
      testid: 'button-vertical-nav-documentation'
    },
    {
      title: 'Swagger',
      path: OpenAPI.BASE + '/swagger-ui/',
      icon: <i className='bx bx-code-curly' />,
      openInNewTab: true,
      testid: 'button-vertical-nav-swagger'
    },
    {
      title: 'Email',
      path: 'mailto:learnmore@feldera.com',
      icon: <i className='bx bx-envelope' />,
      openInNewTab: true,
      testid: 'button-vertical-nav-email'
    },
    {
      title: 'Slack',
      path: 'https://felderacommunity.slack.com',
      icon: <SalesforceSlack width='24' />,
      openInNewTab: true,
      testid: 'button-vertical-nav-slack'
    }
  ].flat() as VerticalNavItemsType
}

export default navigation
