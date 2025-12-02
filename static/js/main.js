import * as UI from './ui.js';
import * as Exports from './exports.js';

// ============================================================
// CONFIGURAÇÃO E ESTADO GLOBAL
// ============================================================

const HERE_API_KEY = window.HERE_API_KEY || "BJYVOFTZ4a9ORwZj8G3MB4e_jG2v5WJECQmIeHUxXvw"; 

let map, behavior, ui, marker;
let platform;
let globalData = [];
let globalFileName = "resultado";
let currentEditingIndex = null;

// ============================================================
// 1. INICIALIZAÇÃO (DOM READY)
// ============================================================
document.addEventListener("DOMContentLoaded", () => {
    // Inicializa HERE Platform se a chave existir
    if (HERE_API_KEY) {
        platform = new H.service.Platform({ 'apikey': HERE_API_KEY });
    } else {
        console.error("HERE_API_KEY não encontrada!");
    }
    
    UI.setupTheme();
    UI.setupPopups();
    
    // Listener de Input de Arquivo
    const fileInput = document.getElementById("fileInput");
    if (fileInput) {
        fileInput.addEventListener("change", function () {
            const fileNameSpan = document.getElementById("fileName");
            if (fileNameSpan) fileNameSpan.textContent = this.files.length ? this.files[0].name : "Nenhum arquivo escolhido";
        });
    }

    // --- CORREÇÃO DO BOTÃO FECHAR ---
    // Adicionamos o evento diretamente aqui e garantimos que ele sobreponha outros eventos
    const btnClose = document.getElementById("btnCloseModal");
    if (btnClose) {
        btnClose.addEventListener("click", (e) => {
            e.preventDefault(); // Evita comportamentos estranhos
            closeMapModal();
        });
    }
});

// Função dedicada para fechar o modal
function closeMapModal() {
    const modal = document.getElementById("mapModal");
    if (modal) {
        modal.classList.add("hidden");
        // Opcional: Limpar o mapa ou redefinir estado se necessário
    }
}

// ============================================================
// 2. UPLOAD E PROCESSAMENTO
// ============================================================
const btnUpload = document.getElementById("btnUpload");
if (btnUpload) {
    btnUpload.onclick = () => {
        const fileInput = document.getElementById("fileInput");
        const file = fileInput.files[0];

        if (!file) {
            alert("Selecione um arquivo Excel primeiro!");
            return;
        }

        globalFileName = file.name.split('.')[0] + "_processado";
        
        UI.toggleUploadState(true);
        UI.updateProgress(0, "Carregando arquivo...");

        const formData = new FormData();
        formData.append("file", file);

        const xhr = new XMLHttpRequest();

        xhr.upload.onprogress = (event) => {
            if (event.lengthComputable) {
                const pct = Math.round((event.loaded / event.total) * 100);
                UI.updateProgress(pct, pct === 100 ? "Processando dados (Aguarde)..." : null);
            }
        };

        xhr.onload = () => {
            UI.toggleUploadState(false);
            
            if (xhr.status >= 200 && xhr.status < 300) {
                try {
                    const resp = JSON.parse(xhr.responseText);
                    handleSuccess(resp);
                } catch (e) {
                    alert("Erro ao ler resposta do servidor.");
                    console.error(e);
                }
            } else {
                alert("Erro ao processar: " + xhr.statusText);
            }
        };

        xhr.onerror = () => {
            UI.toggleUploadState(false);
            alert("Erro de conexão.");
        };

        xhr.open("POST", "/upload", true);
        xhr.send(formData);
    };
}

function handleSuccess(resp) {
    let data = resp.data;

    data = data.map(row => {
        const cleaned = { ...row };
        delete cleaned["Unnamed: 0"];
        return cleaned;
    });

    data = data.filter(row => Object.values(row).some(v => v !== null && v !== "" && v !== undefined));

    globalData = data;
    
    const resultDiv = document.getElementById("result");
    if(resultDiv) resultDiv.classList.remove("hidden");
    
    UI.updateStats(data);
    UI.updateTitle(data);
    
    const columns = data.length ? Object.keys(data[0]) : [];
    UI.renderTable(columns, data); 
}

// ============================================================
// 3. EXPORTAÇÃO
// ============================================================
const btnExpExcel = document.getElementById("popupExportExcel");
if (btnExpExcel) {
    btnExpExcel.onclick = () => {
        document.getElementById("exportPopup").classList.add("hidden");
        document.getElementById("exportPopup").classList.remove("flex");
        Exports.exportToExcel(globalData, globalFileName);
    };
}

const btnExpCircuit = document.getElementById("popupExportCircuit");
if (btnExpCircuit) {
    btnExpCircuit.onclick = () => {
        document.getElementById("exportPopup").classList.add("hidden");
        document.getElementById("exportPopup").classList.remove("flex");
        Exports.exportToCircuit(globalData, globalFileName);
    };
}

// ============================================================
// 4. MAPA, BUSCA E EDIÇÃO
// ============================================================

window.openEditor = function(index) {
    currentEditingIndex = index;
    const row = globalData[index];
    
    const address = row['Destination Address'] || row['Endereço'] || row['Address'] || row['Rua'] || "";
    
    const modal = document.getElementById("mapModal");
    modal.classList.remove("hidden");
    
    document.getElementById("modalAddressLabel").textContent = address;
    const manualInput = document.getElementById("manualSearchInput");
    if(manualInput) manualInput.value = "";
    
    document.getElementById("btnConfirmLocation").disabled = true;
    document.getElementById("selectedCoords").textContent = "Clique no mapa...";

    // Inicializa Mapa apenas uma vez
    if (!map) {
        initMap();
    }

    setTimeout(() => {
        if(map) {
            map.getViewPort().resize();
            
            let startLat = parseFloat(row["Geo_Latitude"]);
            let startLng = parseFloat(row["Geo_Longitude"]);
            
            if (!isNaN(startLat) && !isNaN(startLng)) {
                const latLng = {lat: startLat, lng: startLng};
                map.setCenter(latLng);
                map.setZoom(16);
                updateMarker(latLng);
            } else {
                map.setCenter({ lat: -16.6868, lng: -49.2647 });
                map.setZoom(13);
                if (marker) map.removeObject(marker);
            }
        }
    }, 300);
};

function initMap() {
    const mapContainer = document.getElementById('mapContainer');
    
    // Verificação de segurança
    if (!mapContainer) return;

    // Limpa o container caso haja lixo de renderizações anteriores
    mapContainer.innerHTML = '';

    const defaultLayers = platform.createDefaultLayers();
    
    map = new H.Map(mapContainer, defaultLayers.vector.normal.map, {
        zoom: 13,
        center: { lat: -16.6868, lng: -49.2647 },
        pixelRatio: window.devicePixelRatio || 1
    });
    
    window.addEventListener('resize', () => map.getViewPort().resize());
    
    behavior = new H.mapevents.Behavior(new H.mapevents.MapEvents(map));

    ui = H.ui.UI.createDefault(map, defaultLayers);

    map.addEventListener('tap', function(evt) {
        const coord = map.screenToGeo(evt.currentPointer.viewportX, evt.currentPointer.viewportY);
        updateSelectedInfo(coord.lat, coord.lng);
    });
}

function updateMarker(coord) {
    if (marker) map.removeObject(marker);
    marker = new H.map.Marker(coord);
    map.addObject(marker);
}

function updateSelectedInfo(lat, lng) {
    updateMarker({lat, lng});
    document.getElementById("selectedCoords").textContent = `${lat.toFixed(5)}, ${lng.toFixed(5)}`;
    
    const btnConfirm = document.getElementById("btnConfirmLocation");
    btnConfirm.disabled = false;
    btnConfirm.dataset.lat = lat;
    btnConfirm.dataset.lng = lng;
}

// --- Botão Confirmar ---
const btnConfirm = document.getElementById("btnConfirmLocation");
if (btnConfirm) {
    btnConfirm.onclick = function() {
        if (currentEditingIndex === null) return;

        const lat = parseFloat(this.dataset.lat);
        const lng = parseFloat(this.dataset.lng);

        globalData[currentEditingIndex]["Geo_Latitude"] = lat;
        globalData[currentEditingIndex]["Geo_Longitude"] = lng;
        globalData[currentEditingIndex]["Status_Log"] = "MANUAL_FIX";
        globalData[currentEditingIndex]["Partial_Match"] = false;

        const columns = globalData.length ? Object.keys(globalData[0]) : [];
        UI.renderTable(columns, globalData); 

        closeMapModal(); // Usa a função de fechar corrigida
    };
}

// --- Busca Manual ---
const btnManual = document.getElementById("btnManualSearch");
if (btnManual) {
    btnManual.onclick = () => performManualSearch();
}

const inputManual = document.getElementById("manualSearchInput");
if (inputManual) {
    inputManual.addEventListener("keypress", function(event) {
        if (event.key === "Enter") {
            event.preventDefault();
            performManualSearch();
        }
    });
}

function performManualSearch() {
    const query = document.getElementById("manualSearchInput").value;
    if (!query) return;

    const btn = document.getElementById("btnManualSearch");
    const originalText = btn.innerText;
    btn.innerText = "...";
    btn.disabled = true;

    const service = platform.getSearchService();

    service.geocode({ q: query }, (result) => {
        btn.innerText = originalText;
        btn.disabled = false;

        if (result.items.length > 0) {
            const pos = result.items[0].position;
            map.setCenter(pos);
            map.setZoom(16);
            updateSelectedInfo(pos.lat, pos.lng);
        } else {
            alert("Endereço não encontrado.");
        }
    }, (error) => {
        console.error(error);
        btn.innerText = originalText;
        btn.disabled = false;
        alert("Erro na busca.");
    });
}