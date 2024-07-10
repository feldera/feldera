import { base } from '$app/paths'
import { felderaEndpoint } from '$lib/functions/configs/felderaEndpoint'

import type { VerticalNavItemsType } from '$lib/types/layout'

// TODO: remove conditional Settings page link display
// after more content is added to Settings page
// so it is never empty
export const verticalNavItems = (props: { showSettings: boolean }): VerticalNavItemsType => {
  return [
    // {
    //   title: 'Home',
    //   path: `${base}/home/`,
    //   class: 'bx bx-home-circle',
    //   testid: 'button-vertical-nav-home'
    // },
    {
      sectionTitle: 'Analytics'
    },
    {
      title: 'Pipelines',
      path: [`${base}/pipelines/`],
      class: 'bx bx-git-repo-forked',
      testid: 'button-vertical-nav-pipelines'
    },
    {
      title: 'Demos',
      path: `${base}/demos/`,
      class: 'bx bx-rocket'
    },
    {
      sectionTitle: 'Platform'
    },
    [
      props.showSettings
        ? [
            {
              title: 'Settings',
              path: `${base}/settings/`,
              class: 'bx bx-cog'
            }
          ]
        : []
    ],
    {
      title: 'Documentation',
      path: 'https://www.feldera.com/docs/',
      class: 'bx bx-file',
      openInNewTab: true,
      testid: 'button-vertical-nav-documentation'
    },
    {
      title: 'Swagger',
      path: felderaEndpoint + '/swagger-ui/',
      class: 'bx bx-code-curly',
      openInNewTab: true,
      testid: 'button-vertical-nav-swagger'
    },
    {
      title: 'Email',
      path: 'mailto:learnmore@feldera.com',
      class: 'bx bx-envelope',
      openInNewTab: true,
      testid: 'button-vertical-nav-email'
    },
    {
      title: 'Discord',
      path: 'https://felderacommunity.slack.com',
      class: 'font-brands fa-discord w-6  before:-ml-0.5',
      openInNewTab: true,
      testid: 'button-vertical-nav-discord'
    },
    {
      title: 'Slack',
      path: 'https://felderacommunity.slack.com',
      class: 'font-brands fa-slack w-6 before:ml-0.5',
      openInNewTab: true,
      testid: 'button-vertical-nav-slack'
    }
  ].flat(2) as VerticalNavItemsType
}
