(function () {
  try {
    const qp = new URLSearchParams(location.search);
    if (qp.get('viewport') === 'mobile') {
      document.documentElement.classList.add('force-mobile');
    }
  } catch (e) {
    // no-op
  }
})();
