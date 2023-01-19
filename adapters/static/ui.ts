export interface IHtmlElement {
    getHTMLRepresentation(): HTMLElement;
}

export function formatJson(json: any): string {
    return JSON.stringify(json, null, 4);
}

/**
 * A list of special unicode character codes.
 */
export class SpecialChars {
    // Approximation sign.
    public static approx = "\u2248";
    public static upArrow = "▲";
    public static downArrow = "▼";
    public static rightArrow = "⇒";
    public static ellipsis = "…";
    public static downArrowHtml = "&dArr;";
    public static upArrowHtml = "&uArr;";
    public static leftArrowHtml = "&lArr;";
    public static rightArrowHtml = "&rArr;";
    public static epsilon = "\u03B5";
    public static enDash = "&ndash;";
    public static scissors = "\u2702";
}

export function px(dim: number): string {
    if (dim === 0)
        return dim.toString();
    return dim.toString() + "px";
}

/**
 * Remove all children of an HTML DOM object..
 */
export function removeAllChildren(h: HTMLElement): void {
    while (h.lastChild != null)
        h.removeChild(h.lastChild);
}

export function beep() {
    const audioCtx = new window.AudioContext();
    var oscillator = audioCtx.createOscillator();
    var gainNode = audioCtx.createGain();
    oscillator.connect(gainNode);
    gainNode.connect(audioCtx.destination);
    gainNode.gain.value = 10;
    oscillator.frequency.value = 800;
    oscillator.type = "sine";
    oscillator.start();
    setTimeout(
      function() {
        oscillator.stop();
      },
      100
    );
  };