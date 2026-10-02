export function register(ctx) {
  ctx.refreshUserAvatar = async function refreshUserAvatar(refresh = false) {
    const avatar = document.getElementById("userAvatar");
    if (!avatar?.dataset.profileUrl) return;
    const url = new URL(avatar.dataset.profileUrl, window.location.href);
    if (refresh) url.searchParams.set("refresh", "1");
    try {
      const response = await fetch(url, { credentials: "same-origin" });
      if (!response.ok) return;
      const profile = await response.json();
      if (!profile.ok) return;
      if (typeof profile.account_label === "string") {
        const label = document.getElementById("userAccountLabel");
        if (label) label.textContent = profile.account_label;
      }
      if (profile.surname) {
        avatar.textContent = profile.surname;
        avatar.dataset.avatarLength = String([...profile.surname].length);
      }
    } catch {
      // Profile lookup must not interrupt the assignment workspace.
    }
  };
}

export function initialize(ctx) {
  void ctx.refreshUserAvatar();
}
