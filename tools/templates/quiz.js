function dbxQuiz(root, questions) {
  if (typeof root === 'string') root = document.getElementById(root);
  var i = 0, score = 0, picked = null, shown = false;
  function esc(s) { return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/`([^`]+)`/g, '<code>$1</code>'); }
  function btn(label, primary) { return '<button style="margin-top:14px;padding:8px 18px;border-radius:4px;border:none;cursor:pointer;background:' + (primary ? '#2272B4' : '#E0E0E0') + ';color:' + (primary ? '#fff' : '#333') + ';font-size:0.95em">' + label + '</button>'; }
  function render() {
    if (i >= questions.length) {
      root.innerHTML = '<div style="padding:20px;border:1px solid #E0E0E0;border-radius:8px;text-align:center;background:#fff"><div style="font-size:1.3em;font-weight:bold;color:#333">Score: ' + score + ' / ' + questions.length + '</div>' + btn('Retake', true) + '</div>';
      root.querySelector('button').onclick = function () { i = 0; score = 0; picked = null; shown = false; render(); };
      return;
    }
    var Q = questions[i], h = '<div style="padding:20px;border:1px solid #E0E0E0;border-radius:8px;background:#fff">';
    h += '<div style="color:#999;font-size:0.85em;margin-bottom:6px">Question ' + (i + 1) + ' of ' + questions.length + '</div>';
    h += '<div style="font-weight:bold;color:#333;margin-bottom:12px;line-height:1.5">' + esc(Q.q) + '</div>';
    Q.o.forEach(function (t, k) {
      var bg = '#fff', bd = '#e0e0e0';
      if (shown && k === Q.c) { bg = '#E8F5E9'; bd = '#43A047'; }
      else if (shown && k === picked) { bg = '#FFEBEE'; bd = '#E53935'; }
      else if (!shown && k === picked) { bg = '#E3F2FD'; bd = '#2272B4'; }
      h += '<div data-k="' + k + '" style="padding:10px 14px;margin:6px 0;border:2px solid ' + bd + ';background:' + bg + ';border-radius:6px;cursor:' + (shown ? 'default' : 'pointer') + ';color:#333">' + String.fromCharCode(97 + k) + ') ' + esc(t) + '</div>';
    });
    if (shown) {
      h += '<div style="margin-top:12px;padding:12px;background:#F5F5F5;border-radius:6px;color:#333"><strong>' + (picked === Q.c ? '&#9989; Correct.' : '&#10060; Not quite.') + '</strong> ' + esc(Q.e) + '</div>' + btn(i + 1 < questions.length ? 'Next' : 'See score', true);
    } else {
      h += btn('Submit', picked !== null);
    }
    root.innerHTML = h + '</div>';
    if (!shown) root.querySelectorAll('[data-k]').forEach(function (el) { el.onclick = function () { picked = +el.getAttribute('data-k'); render(); }; });
    root.querySelector('button').onclick = function () {
      if (!shown) { if (picked === null) return; if (picked === Q.c) score++; shown = true; }
      else { i++; picked = null; shown = false; }
      render();
    };
  }
  render();
}
