/** Feldera's "book a demo" Calendly event. */
const DEMO_URL = 'https://calendly.com/d/cxz2-37b-qqd/feldera-demo-30min'

/**
 * Calendly stores its query parameters with the booking and forwards them to
 * everything downstream (Zapier, then HubSpot), so the analytics visitor ID
 * travels through them and ties the booking back to the visitor's
 * earlier activity.
 *
 * If the original link ever includes 'utm_content' - carry `visitorId` in 'salesforce_uuid'
 */
const VISITOR_ID_PARAM = 'utm_content'

/**
 * Which button the visitor booked from, e.g. 'try.feldera.com:pipeline_editor'.
 * Rides the same route as the visitor ID, so sales sees the page that earned
 * the booking. The part after the prefix matches the `placement` property of
 * our analytics events.
 */
const PLACEMENT_PARAM = 'utm_term'

// Every property that books demos feeds the same Calendly event, so the web-console
// namespaces its placements to distinguish from the website's.
const PLACEMENT_PREFIX = 'try.feldera.com:'

/**
 * Tag the link, reporting collisions with any existing query params.
 */
const setTrackingParam = (url: URL, param: string, value: string) => {
  if (url.searchParams.has(param)) {
    console.warn(
      `Calendly demo link already carries '${param}' ` +
        `('${url.searchParams.get(param)}'); overwriting it with '${value}'. ` +
        `Move our value to a free parameter, e.g. 'salesforce_uuid', and remap it in Zapier.`
    )
  }
  url.searchParams.set(param, value)
}

/**
 * The demo link, tagged with the analytics visitor ID and the placement of the
 * button that opens it. An empty ID leaves the link unattributed: the booking
 * still works, it just does not tie back to the visitor.
 * Needs to be used within $derived to update the URL
 * when the conceptualHq.deviceId becomes available
 */
export const bookADemoUrl = ({
  visitorId,
  placement
}: {
  visitorId: string
  placement?: string
}): string => {
  if (!visitorId && !placement) {
    return DEMO_URL
  }
  const url = new URL(DEMO_URL)
  if (visitorId) {
    setTrackingParam(url, VISITOR_ID_PARAM, visitorId)
  }
  if (placement) {
    setTrackingParam(url, PLACEMENT_PARAM, PLACEMENT_PREFIX + placement)
  }
  return url.toString()
}
