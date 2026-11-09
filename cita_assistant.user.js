// ==UserScript==
// @name         Cita Previa Extranjería Assistant
// @namespace    http://tampermonkey.net/
// @version      1.1
// @description  Automates navigation, form filling, and audio alerting for Spanish Extranjería appointments
// @match        https://icp.administracionelectronica.gob.es/icpplus/*
// @grant        none
// ==/UserScript==

(function() {
    'use strict';

    // ==============================================================
    // CONFIGURATION: Personal & Procedure Information
    // ==============================================================
    const CONFIG = {
        province: "Barcelona",
        procedureKeyword: "POLICIA-TOMA DE HUELLA",
        docType: "NIE", // "NIE" or "PASAPORTE"
        docNumber: "Z5068943R",
        fullName: "MD MUSTAK AHMED",
        yearOfBirth: "1990",
        countryOfNationality: "BANGLADESH",
        phone: "698293839",
        email: "mdmus@postgot.com"
    };

    function playAlertSound() {
        try {
            const ctx = new (window.AudioContext || window.webkitAudioContext)();
            for (let i = 0; i < 4; i++) {
                const osc = ctx.createOscillator();
                const gain = ctx.createGain();
                osc.connect(gain);
                gain.connect(ctx.destination);
                osc.frequency.setValueAtTime(880 + (i % 2 === 0 ? 200 : 0), ctx.currentTime + i * 0.3);
                gain.gain.setValueAtTime(0.3, ctx.currentTime + i * 0.3);
                osc.start(ctx.currentTime + i * 0.3);
                osc.stop(ctx.currentTime + i * 0.3 + 0.2);
            }
        } catch (e) {
            console.error("Audio playback error:", e);
        }
    }

    function processPage() {
        const bodyText = document.body.innerText;

        // Step 1: Province Selection
        const provinceSelect = document.getElementById('form') || document.querySelector('select[name="sede"]');
        if (provinceSelect) {
            for (let opt of provinceSelect.options) {
                if (opt.text.toLowerCase().includes(CONFIG.province.toLowerCase())) {
                    provinceSelect.value = opt.value;
                    const btn = document.getElementById('btnAceptar') || document.querySelector('input[type="submit"]');
                    if (btn) btn.click();
                    return;
                }
            }
        }

        // Step 2: Procedure (Trámite) Selection
        const procedureSelect = document.getElementById('tramiteGrupo[0]') || document.querySelector('select[name="tramite"]');
        if (procedureSelect) {
            for (let opt of procedureSelect.options) {
                if (opt.text.toUpperCase().includes(CONFIG.procedureKeyword.toUpperCase())) {
                    procedureSelect.value = opt.value;
                    const btn = document.getElementById('btnAceptar') || document.querySelector('input[type="submit"]');
                    if (btn) btn.click();
                    return;
                }
            }
        }

        // Step 3: Information page acknowledgment
        const entrarBtn = document.getElementById('btnEntrar');
        if (entrarBtn) {
            entrarBtn.click();
            return;
        }

        // Step 4: Identity Details
        const nieInput = document.getElementById('txtIdCitado');
        const nameInput = document.getElementById('txtDesCitado');
        if (nieInput && nameInput) {
            if (CONFIG.docType === "NIE") {
                const nieRadio = document.getElementById('rdbTipoDocNie');
                if (nieRadio) nieRadio.checked = true;
            } else if (CONFIG.docType === "PASAPORTE") {
                const pasRadio = document.getElementById('rdbTipoDocPas');
                if (pasRadio) pasRadio.checked = true;
            }
            nieInput.value = CONFIG.docNumber;
            nameInput.value = CONFIG.fullName;

            const yearInput = document.getElementById('txtAnnoCitado');
            if (yearInput) yearInput.value = CONFIG.yearOfBirth;

            const countrySelect = document.getElementById('txtPaisNac');
            if (countrySelect) {
                for (let opt of countrySelect.options) {
                    if (opt.text.toUpperCase().includes(CONFIG.countryOfNationality.toUpperCase())) {
                        countrySelect.value = opt.value;
                        break;
                    }
                }
            }
            console.log("Personal details auto-filled. Solve CAPTCHA if prompted, then click Submit.");
            return;
        }

        // Step 5: Contact Details (Phone & Email)
        const phoneInput = document.getElementById('txtTelefonoCitado');
        const emailInput = document.getElementById('emailCMP');
        if (phoneInput || emailInput) {
            if (phoneInput) phoneInput.value = CONFIG.phone;
            if (emailInput) emailInput.value = CONFIG.email;
            const emailConfirm = document.getElementById('emailCMP2');
            if (emailConfirm) emailConfirm.value = CONFIG.email;
            return;
        }

        // Step 6: Slot Detection
        if (bodyText.includes("En este momento no hay citas disponibles")) {
            console.warn("Extranjería: No appointment slots available currently.");
        } else if (document.querySelector('input[name="cita"]') || bodyText.includes("Seleccione la cita")) {
            playAlertSound();
            document.title = "🚨 CITA DISPONIBLE! 🚨";
            alert("Appointment slot detected! Choose your slot and enter the SMS code.");
        }
    }

    window.addEventListener('load', () => setTimeout(processPage, 1000));
})();

