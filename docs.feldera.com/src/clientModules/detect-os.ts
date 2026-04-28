// Pre-selects the OS tab (`<Tabs groupId="os">`) based on the visitor's
// platform, so first-time visitors see install instructions for their own
// operating system. Docusaurus persists tab choice under
// `docusaurus.tab.<groupId>`; setting that key before the Tabs component
// hydrates makes it pick our value as the default. We never overwrite an
// existing value, so a user's manual pick continues to win.

const STORAGE_KEY = "docusaurus.tab.os";

function detectOs(): "linux" | "macos" | "windows" | undefined {
  const ua = navigator.userAgent || "";
  const platform = (navigator.platform || "").toLowerCase();

  if (/Mac|Darwin/i.test(ua) || platform.includes("mac")) return "macos";
  if (/Win/i.test(ua) || platform.includes("win")) return "windows";
  if (/Linux|X11|CrOS/i.test(ua)) return "linux";
  return undefined;
}

if (typeof window !== "undefined") {
  try {
    if (!localStorage.getItem(STORAGE_KEY)) {
      const os = detectOs();
      if (os) localStorage.setItem(STORAGE_KEY, os);
    }
  } catch {
    // localStorage may be unavailable (private mode, disabled cookies); ignore.
  }
}

export {};
