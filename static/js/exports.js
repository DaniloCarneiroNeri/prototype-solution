// ============================================================
// EXPORTS.JS - Lógica de geração de arquivos
// ============================================================

export function exportToExcel(data, fileName) {
    if (!data || !data.length) return alert("Não há dados para exportar!");
    
    if (typeof XLSX === 'undefined') return alert("Biblioteca XLSX não carregada.");

    const sheetEncontrados = [];
    const sheetParciais = [];
    const sheetCondominios = [];
    const sheetNaoEncontrados = [];

    data.forEach(item => {
        const isCondo = item.Cond_Match;
        const isPartial = item.Partial_Match;
        
        const hasLat = item.Geo_Latitude && item.Geo_Longitude !== "Não encontrado";

        if (isCondo) {
            sheetCondominios.push(item);
        } else if (isPartial) {
            sheetParciais.push(item);
        } else if (hasLat) {
            sheetEncontrados.push(item);
        } else {
            sheetNaoEncontrados.push(item);
        }
    });

    const wb = XLSX.utils.book_new();

    function appendIfData(dataset, sheetName) {
        if (dataset.length > 0) {
            const ws = XLSX.utils.json_to_sheet(dataset);
            const safeName = sheetName.replace(/[:\\/?*\[\]]/g, " ").substring(0, 31);
            XLSX.utils.book_append_sheet(wb, ws, safeName);
        }
    }

    appendIfData(sheetEncontrados, "Encontrados");
    appendIfData(sheetParciais, "Encontrados Parcialmente");
    appendIfData(sheetCondominios, "Condominios Identificados");
    appendIfData(sheetNaoEncontrados, "Nao Encontrados - Erros"); 

    if (wb.SheetNames.length === 0) {
        return alert("Nenhum dado válido encontrado para gerar as abas.");
    }

    console.log(`Exportando arquivo com ${wb.SheetNames.length} abas.`);

    XLSX.writeFile(wb, `${fileName}.xlsx`);
}

export function exportToCircuit(data, fileName) {
    if (!data || !data.length) return alert("Nenhum dado carregado!");

    const grouped = new Map();

    data.forEach((r, i) => {
        const lat = r["Geo_Latitude"];
        const lng = r["Geo_Longitude"];

        if (lat === "Não encontrado" || lng === "Não encontrado") return;
        if (!lat || !lng) return;

        const normalized = r["Normalized_Address"] || "";
        let quadra = "";
        let lote = "";

        const match = normalized.match(/,\s*([0-9]+)-([0-9]+)/);
        if (match) {
            quadra = match[1];
            lote = match[2];
        }

        const seq = i + 1;
        const key = `${lat}|${lng}`;

        if (grouped.has(key)) {
            const existing = grouped.get(key);
            existing.Sequencias.push(seq);
        } else {
            grouped.set(key, {
                Geo_Latitude: lat,
                Geo_Longitude: lng,
                Quadra: quadra,
                Lote: lote,
                Sequencias: [seq]
            });
        }
    });

    const rows = Array.from(grouped.values());

    if (rows.length === 0) {
        alert("Nenhum dado válido encontrado para exportação.");
        return;
    }

    let csv = "Geo_Latitude,Observacoes\n";

    rows.forEach(r => {
        const seqStr = r.Sequencias.join(", ");
        const obs = `${seqStr} - Quadra:${r.Quadra} - Lote:${r.Lote}`;
        csv += `${r.Geo_Latitude}, ${r.Geo_Longitude},"${obs}"\n`;
    });

    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);

    const a = document.createElement("a");
    a.href = url;
    a.download = `${fileName}_CIRCUIT.csv`;
    a.click();

    URL.revokeObjectURL(url);
}