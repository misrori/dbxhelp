function dbxQuiz(root, questions, onDone) {
  if (typeof root === 'string') root = document.getElementById(root);
  var i = 0, score = 0, picked = null, shown = false, done = false;
  function esc(s) { return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/`([^`]+)`/g, '<code style="background:#EEF2FF;color:#3730A3;padding:1px 6px;border-radius:6px">$1</code>'); }
  function btn(label, primary) { return '<button style="margin-top:16px;padding:10px 22px;border-radius:10px;border:none;cursor:pointer;font-weight:700;background:' + (primary ? 'linear-gradient(135deg,#FF3621,#7C4DFF)' : '#E5E7EB') + ';color:' + (primary ? '#fff' : '#6B7280') + ';font-size:0.95em;box-shadow:' + (primary ? '0 6px 18px rgba(124,77,255,.3)' : 'none') + '">' + label + '</button>'; }
  var box = 'padding:22px 24px;border-radius:16px;background:#fff;border:1px solid #E5E7EB;box-shadow:0 8px 28px rgba(27,49,57,.08);animation:dbxQ .4s ease both';
  var style = '<style>@keyframes dbxQ{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:none}}</style>';
  function bar() {
    var h = '<div style="display:flex;gap:6px;margin-bottom:14px">';
    for (var k = 0; k < questions.length; k++) h += '<div style="flex:1;height:6px;border-radius:6px;background:' + (k < i ? 'linear-gradient(90deg,#FF3621,#7C4DFF)' : k === i ? '#C4B5FD' : '#E5E7EB') + '"></div>';
    return h + '</div>';
  }
  function render() {
    if (i >= questions.length) {
      var pct = Math.round(score / questions.length * 100), msg = pct === 100 ? 'Perfect! 🎉' : pct >= 60 ? 'Nice work! 💪' : 'Worth another look 📖';
      root.innerHTML = style + '<div style="' + box + ';text-align:center"><div style="font-size:48px">' + (pct === 100 ? '🏆' : pct >= 60 ? '🌟' : '🔁') + '</div><div style="font-size:1.6em;font-weight:800;color:#1B3139">' + score + ' / ' + questions.length + '</div><div style="color:#6B7280;margin-top:4px">' + msg + '</div>' + btn('Retake', true) + '</div>';
      root.querySelector('button').onclick = function () { i = 0; score = 0; picked = null; shown = false; done = false; render(); };
      if (!done && onDone) { done = true; onDone(score, questions.length); }
      return;
    }
    var Q = questions[i], h = style + '<div style="' + box + '">' + bar();
    h += '<div style="color:#7C4DFF;font-size:0.8em;font-weight:800;letter-spacing:.08em;text-transform:uppercase;margin-bottom:6px">Question ' + (i + 1) + ' of ' + questions.length + '</div>';
    h += '<div style="font-weight:700;color:#1B3139;margin-bottom:14px;line-height:1.5;font-size:1.05em">' + esc(Q.q) + '</div>';
    Q.o.forEach(function (t, k) {
      var bg = '#fff', bd = '#E5E7EB', mark = String.fromCharCode(65 + k);
      if (shown && k === Q.c) { bg = '#ECFDF3'; bd = '#22C55E'; mark = '✓'; }
      else if (shown && k === picked) { bg = '#FFF1F0'; bd = '#EF4444'; mark = '✗'; }
      else if (!shown && k === picked) { bg = '#F5F3FF'; bd = '#7C4DFF'; }
      h += '<div data-k="' + k + '" style="display:flex;gap:12px;align-items:center;padding:11px 14px;margin:8px 0;border:2px solid ' + bd + ';background:' + bg + ';border-radius:12px;cursor:' + (shown ? 'default' : 'pointer') + ';color:#1F2937;transition:all .15s">' +
        '<span style="flex:none;width:28px;height:28px;border-radius:8px;display:inline-flex;align-items:center;justify-content:center;font-weight:800;font-size:.85em;background:' + (bd === '#E5E7EB' ? '#F1F5F9' : bd) + ';color:' + (bd === '#E5E7EB' ? '#64748B' : '#fff') + '">' + mark + '</span><span>' + esc(t) + '</span></div>';
    });
    if (shown) {
      var ok = picked === Q.c;
      h += '<div style="margin-top:14px;padding:14px 16px;border-radius:12px;background:' + (ok ? '#ECFDF3' : '#FFF7ED') + ';border:1px solid ' + (ok ? '#A7F3D0' : '#FED7AA') + ';color:#1F2937;animation:dbxQ .3s both"><strong>' + (ok ? '✅ Correct.' : '💡 Not quite.') + '</strong> ' + esc(Q.e) + '</div>' + btn(i + 1 < questions.length ? 'Next →' : 'See score', true);
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
