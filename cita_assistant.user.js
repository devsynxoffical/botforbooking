// ==UserScript==
// @name         Cita Previa Extranjería - Urban VPN Auto-Booker Assistant
// @namespace    http://devsynx.com/
// @version      2.5
// @description  Automates navigation, form filling, slot claiming, and audio alerts for Spanish Extranjería appointments using Urban VPN in Chrome.
// @match        https://icp.administracionelectronica.gob.es/icpplus/*
// @grant        none
// ==/UserScript==

(function() {
    'use strict';

    const QUEUE_API = "https://web-production-2bcf4.up.railway.app/api/orders?status=waiting";

    // Default configuration (will be populated from Railway queue or dropdown)
    let CONFIG = {
        orderId: "",
        province: "Barcelona",
        procedureKeyword: "POLICIA-TOMA DE HUELLA",
        docType: "NIE",
        docNumber: "",
        fullName: "",
        yearOfBirth: "1990",
        countryOfNationality: "BANGLADESH",
        phone: "",
        email: "",
        autoAdvance: true
    };

    // Load persisted config from localStorage
    try {
        const saved = localStorage.getItem("CITA_ASSISTANT_CONFIG");
        if (saved) {
            CONFIG = Object.assign(CONFIG, JSON.parse(saved));
        }
    } catch (e) {}

    function saveConfig() {
        try {
            localStorage.setItem("CITA_ASSISTANT_CONFIG", JSON.stringify(CONFIG));
        } catch (e) {}
    }

    function playAlertSound() {
        try {
            const ctx = new (window.AudioContext || window.webkitAudioContext)();
            for (let i = 0; i < 5; i++) {
                const osc = ctx.createOscillator();
                const gain = ctx.createGain();
                osc.connect(gain);
                gain.connect(ctx.destination);
                osc.frequency.setValueAtTime(880 + (i % 2 === 0 ? 300 : 0), ctx.currentTime + i * 0.25);
                gain.gain.setValueAtTime(0.4, ctx.currentTime + i * 0.25);
                osc.start(ctx.currentTime + i * 0.25);
                osc.stop(ctx.currentTime + i * 0.25 + 0.2);
            }
        } catch (e) {
            console.error("Audio error:", e);
        }
    }

    // Build Floating Toolbar UI
    function injectFloatingUI() {
        if (document.getElementById("cita-bot-toolbar")) return;

        const container = document.createElement("div");
        container.id = "cita-bot-toolbar";
        container.style.cssText = `
            position: fixed;
            top: 12px;
            right: 12px;
            width: 330px;
            background: #0f172a;
            color: #f8fafc;
            border-radius: 12px;
            box-shadow: 0 10px 25px -5px rgba(0,0,0,0.5), 0 0 0 1px #334155;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            font-size: 13px;
            z-index: 9999999;
            padding: 14px;
            line-height: 1.4;
        `;

        container.innerHTML = `
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom: 10px; border-bottom: 1px solid #334155; padding-bottom: 8px;">
                <div style="font-weight: 700; font-size: 14px; color: #38bdf8; display:flex; align-items:center; gap:6px;">
                    <span>🇪🇸</span> Cita Auto-Booker
                </div>
                <span style="font-size: 11px; background: #166534; color: #86efac; padding: 2px 8px; border-radius: 999px; font-weight: 600;">Urban VPN Active</span>
            </div>

            <div style="margin-bottom: 10px;">
                <label style="display:block; font-size: 11px; color: #94a3b8; font-weight: 600; text-transform: uppercase; margin-bottom: 4px;">Queue Client</label>
                <select id="cita-client-select" style="width:100%; padding: 6px; background: #1e293b; color: white; border: 1px solid #475569; border-radius: 6px; font-size: 12px;">
                    <option value="">-- Load Waiting Orders --</option>
                </select>
                <button id="cita-btn-fetch" style="margin-top: 5px; width: 100%; background: #2563eb; color: white; border: none; padding: 6px; border-radius: 6px; font-size: 11px; font-weight: 600; cursor: pointer;">
                    🔄 Refresh Queue from Railway
                </button>
            </div>

            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 6px; margin-bottom: 10px;">
                <div>
                    <label style="font-size: 10px; color: #94a3b8; font-weight:600;">NIE / PASSPORT</label>
                    <input id="cita-cfg-nie" type="text" value="${CONFIG.docNumber || ''}" placeholder="Z1234567X" style="width:100%; box-sizing:border-box; padding:4px 6px; background:#1e293b; color:white; border:1px solid #475569; border-radius:4px; font-size:11px;" />
                </div>
                <div>
                    <label style="font-size: 10px; color: #94a3b8; font-weight:600;">BIRTH YEAR</label>
                    <input id="cita-cfg-year" type="text" value="${CONFIG.yearOfBirth || '1990'}" placeholder="1990" style="width:100%; box-sizing:border-box; padding:4px 6px; background:#1e293b; color:white; border:1px solid #475569; border-radius:4px; font-size:11px;" />
                </div>
            </div>

            <div style="margin-bottom: 10px;">
                <label style="font-size: 10px; color: #94a3b8; font-weight:600;">FULL NAME</label>
                <input id="cita-cfg-name" type="text" value="${CONFIG.fullName || ''}" placeholder="Full Name" style="width:100%; box-sizing:border-box; padding:4px 6px; background:#1e293b; color:white; border:1px solid #475569; border-radius:4px; font-size:11px;" />
            </div>

            <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom: 10px; background:#1e293b; padding: 6px 10px; border-radius:6px;">
                <label style="font-size: 12px; font-weight: 600; cursor:pointer;" for="cita-auto-toggle">⚡ Auto-Advance & Fill</label>
                <input id="cita-auto-toggle" type="checkbox" ${CONFIG.autoAdvance ? 'checked' : ''} style="cursor:pointer; width:16px; height:16px;" />
            </div>

            <button id="cita-btn-execute" style="width:100%; background: #16a34a; color: white; border: none; padding: 9px; border-radius: 8px; font-weight: 700; font-size: 13px; cursor: pointer;">
                🚀 RUN AUTO-BOOKING NOW
            </button>
            <div id="cita-status-log" style="margin-top: 8px; font-size: 11px; color: #cbd5e1; max-height: 50px; overflow-y: auto;">Ready.</div>
        `;

        document.body.appendChild(container);

        // Wire events
        const fetchBtn = document.getElementById("cita-btn-fetch");
        const clientSel = document.getElementById("cita-client-select");
        const execBtn = document.getElementById("cita-btn-execute");
        const autoTgl = document.getElementById("cita-auto-toggle");
        const nieInp = document.getElementById("cita-cfg-nie");
        const yearInp = document.getElementById("cita-cfg-year");
        const nameInp = document.getElementById("cita-cfg-name");
        const logBox = document.getElementById("cita-status-log");

        function setLog(msg) {
            if (logBox) logBox.innerText = msg;
            console.log("[Cita-Assistant]", msg);
        }

        async function fetchQueue() {
            setLog("Fetching queue from Railway...");
            try {
                const res = await fetch(QUEUE_API);
                const data = await res.json();
                if (data && data.orders && data.orders.length > 0) {
                    clientSel.innerHTML = `<option value="">-- ${data.orders.length} Clients Waiting --</option>`;
                    data.orders.forEach(o => {
                        const opt = document.createElement("option");
                        opt.value = JSON.stringify(o);
                        opt.text = `${o.name} (${o.doc_number || o.order_id})`;
                        clientSel.appendChild(opt);
                    });
                    setLog(`Loaded ${data.orders.length} waiting clients!`);
                } else {
                    setLog("No waiting clients in queue.");
                }
            } catch (err) {
                setLog("Could not reach Railway API: " + err.message);
            }
        }

        fetchBtn.addEventListener("click", fetchQueue);

        clientSel.addEventListener("change", (e) => {
            if (!e.target.value) return;
            const o = JSON.parse(e.target.value);
            CONFIG.orderId = o.order_id;
            CONFIG.province = o.province || "Barcelona";
            CONFIG.procedureKeyword = o.procedure || "POLICIA-TOMA DE HUELLA";
            CONFIG.docType = o.doc_type || "NIE";
            CONFIG.docNumber = o.doc_number || "";
            CONFIG.fullName = o.name || "";
            CONFIG.yearOfBirth = o.birth_date || "1990";
            CONFIG.countryOfNationality = o.birth_country || o.citizenship || "BANGLADESH";
            CONFIG.phone = o.phone || "";
            CONFIG.email = o.email || "";

            nieInp.value = CONFIG.docNumber;
            nameInp.value = CONFIG.fullName;
            yearInp.value = CONFIG.yearOfBirth;
            saveConfig();
            setLog(`Selected: ${o.name} (${CONFIG.docNumber})`);
        });

        autoTgl.addEventListener("change", () => {
            CONFIG.autoAdvance = autoTgl.checked;
            saveConfig();
        });

        execBtn.addEventListener("click", () => {
            CONFIG.docNumber = nieInp.value.trim();
            CONFIG.fullName = nameInp.value.trim();
            CONFIG.yearOfBirth = yearInp.value.trim();
            saveConfig();
            processPage(true);
        });

        // Automatically fetch queue on initial load if empty
        setTimeout(fetchQueue, 600);
    }

    function processPage(force = false) {
        if (!CONFIG.autoAdvance && !force) return;

        const bodyText = document.body.innerText;

        // 1. Province Dropdown Page (index.html)
        const provSel = document.getElementById('form') || document.querySelector('select[name="sede"]');
        if (provSel) {
            console.log("Processing Province Selection...");
            for (let opt of provSel.options) {
                if (opt.text.toLowerCase().includes(CONFIG.province.toLowerCase())) {
                    provSel.value = opt.value;
                    const btn = document.getElementById('btnAceptar') || document.querySelector('input[type="submit"]');
                    if (btn) {
                        setTimeout(() => btn.click(), 800);
                        return;
                    }
                }
            }
        }

        // 2. Procedure Dropdown Page (citar)
        const procSel = document.getElementById('tramiteGrupo[0]') || document.getElementById('tramiteGrupo[1]') || document.querySelector('select[name*="tramite"]');
        if (procSel) {
            console.log("Processing Procedure Selection...");
            let matched = false;
            const target = (CONFIG.procedureKeyword || "TOMA DE HUELLA").toUpperCase();
            for (let opt of procSel.options) {
                if (opt.text.toUpperCase().includes(target) || target.includes(opt.text.toUpperCase())) {
                    procSel.value = opt.value;
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                for (let opt of procSel.options) {
                    if (opt.text.toUpperCase().includes("HUELLA") || opt.text.toUpperCase().includes("TIE")) {
                        procSel.value = opt.value;
                        matched = true;
                        break;
                    }
                }
            }
            if (matched) {
                const btn = document.getElementById('btnAceptar') || document.querySelector('input[type="submit"]');
                if (btn) {
                    setTimeout(() => btn.click(), 900);
                    return;
                }
            }
        }

        // 3. Information Acknowledgement Page
        const entrarBtn = document.getElementById('btnEntrar') || document.querySelector('input[value="Entrar"]');
        if (entrarBtn) {
            console.log("Clicking Entrar...");
            setTimeout(() => entrarBtn.click(), 700);
            return;
        }

        // 4. Identity Details Page (txtIdCitado, txtDesCitado)
        const nieInput = document.getElementById('txtIdCitado');
        const nameInput = document.getElementById('txtDesCitado');
        if (nieInput && nameInput) {
            console.log("Filling Identity details...");
            if (CONFIG.docType === "NIE") {
                const nieRadio = document.getElementById('rdbTipoDocNie');
                if (nieRadio) nieRadio.checked = true;
            } else {
                const pasRadio = document.getElementById('rdbTipoDocPas');
                if (pasRadio) pasRadio.checked = true;
            }
            if (CONFIG.docNumber) nieInput.value = CONFIG.docNumber;
            if (CONFIG.fullName) nameInput.value = CONFIG.fullName;

            const yearInput = document.getElementById('txtAnnoCitado');
            if (yearInput && CONFIG.yearOfBirth) yearInput.value = CONFIG.yearOfBirth;

            const countrySelect = document.getElementById('txtPaisNac');
            if (countrySelect && CONFIG.countryOfNationality) {
                for (let opt of countrySelect.options) {
                    if (opt.text.toUpperCase().includes(CONFIG.countryOfNationality.toUpperCase())) {
                        countrySelect.value = opt.value;
                        break;
                    }
                }
            }

            const btnEnviar = document.getElementById('btnEnviar') || document.getElementById('btnAceptar') || document.querySelector('input[type="submit"]');
            if (btnEnviar && CONFIG.docNumber) {
                setTimeout(() => btnEnviar.click(), 1200);
            }
            return;
        }

        // 5. Action Selection Page: "Solicitar Cita"
        const btnSolicitar = document.getElementById('btnEnviar') || document.querySelector('input[value="Solicitar Cita"]');
        if (btnSolicitar && (bodyText.includes("Solicitar Cita") || bodyText.includes("Consultar Citas"))) {
            console.log("Clicking Solicitar Cita...");
            setTimeout(() => btnSolicitar.click(), 800);
            return;
        }

        // 6. Contact Details (Phone & Email)
        const phoneInput = document.getElementById('txtTelefonoCitado');
        const emailInput = document.getElementById('emailCMP');
        if (phoneInput || emailInput) {
            console.log("Filling Contact Details...");
            if (phoneInput && CONFIG.phone) phoneInput.value = CONFIG.phone;
            if (emailInput && CONFIG.email) emailInput.value = CONFIG.email;
            const emailConfirm = document.getElementById('emailCMP2');
            if (emailConfirm && CONFIG.email) emailConfirm.value = CONFIG.email;

            const btnSiguiente = document.getElementById('btnSiguiente') || document.querySelector('input[value="Siguiente"]') || document.querySelector('input[type="submit"]');
            if (btnSiguiente && CONFIG.phone) {
                setTimeout(() => btnSiguiente.click(), 1000);
            }
            return;
        }

        // 7. Appointment Slot Availability & Selection
        if (bodyText.includes("En este momento no hay citas disponibles")) {
            console.warn("⚠️ No appointment slots available currently for this procedure.");
            const logBox = document.getElementById("cita-status-log");
            if (logBox) logBox.innerText = "❌ No slots available right now. Waiting for drop...";
            return;
        }

        // Check for slot radio buttons or calendar slots!
        const slotRadio = document.querySelector('input[type="radio"][name*="cita"], input[type="radio"][name*="idCita"]');
        if (slotRadio) {
            console.log("🚨 FREE SLOT DETECTED!");
            slotRadio.checked = true;
            playAlertSound();
            document.title = "🚨 CITA DISPONIBLE DETECTADA! 🚨";
            const btnSiguiente = document.getElementById('btnSiguiente') || document.querySelector('input[value="Siguiente"]') || document.querySelector('input[type="submit"]');
            if (btnSiguiente) {
                setTimeout(() => btnSiguiente.click(), 1200);
            }
            return;
        }

        // 8. SMS Verification Page
        const otpInput = document.getElementById('txtCodigoVerificacion') || document.querySelector('input[name*="codigo"], input[name*="otp"]');
        if (otpInput || bodyText.includes("código que le hemos enviado") || bodyText.includes("SMS")) {
            playAlertSound();
            document.title = "🚨 SMS CODE REQUIRED! 🚨";
            alert("🚨 SMS Verification Code required! Enter the code received on " + (CONFIG.phone || "the client's phone") + " to finalize booking!");
        }
    }

    // Auto-run on load
    window.addEventListener('load', () => {
        injectFloatingUI();
        setTimeout(() => processPage(false), 1200);
    });

})();
