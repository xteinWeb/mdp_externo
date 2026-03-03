const SUPABASE_URL = 'https://impuswcrzzobqdlaksxi.supabase.co';
const SUPABASE_ANON_KEY = 'sb_publishable_wNXnc2YuLbrRDmiyDta7SA_VbZcqAMc';

let supabaseClient;
let flowsCache = { pintura: [], tapiceria: [] };
let currentUserData = null;

// Estados globales para Seguimiento y Filtros
let allTrackingData = [];
let ordersMap = {};
let productsMap = {};
let prodFlowTypeMap = {};
let prodSectionsMap = {};
let sectionToPlantMap = {};

// Normalización agresiva: todo minúsculas, sin espacios, sin guiones ni barras redundantes
function normalizeStr(str) {
    if (!str) return "";
    return str.toString()
        .normalize("NFD").replace(/[\u0300-\u036f]/g, "") // Remueve tildes (acentos)
        .toLowerCase()
        .replace(/\s+/g, '')        // Quita espacios
        .replace(/[\/\-_]/g, '')   // Quita barras, guiones, etc
        .trim();
}

function findNextAllowedSections(currentSecNorm, flowRules, allowedNorms, visited = new Set()) {
    if (visited.has(currentSecNorm)) return [];
    visited.add(currentSecNorm);

    // 1. Obtener candidatos directos (sección que tiene como predecesora la actual)
    let candidates = flowRules.filter(r => normalizeStr(r.predecesora) === currentSecNorm);

    // 2. Gestionar disparadores de Fin de Grupo (FIN G, FIN GA, etc.)
    // Buscamos TODAS las reglas que coincidan con la sección actual para no omitir grupos si se repiten
    const currentRules = flowRules.filter(r => normalizeStr(r.seccion) === currentSecNorm);

    currentRules.forEach(currRule => {
        if (currRule.final_seccion) {
            // El trigger puede ser "FIN G", "FIN GA", etc. 
            // Lo normalizamos para comparar con los campos 'predecesora'
            const groupTriggerNorm = normalizeStr(`FIN ${currRule.grupo}`);

            const groupNext = flowRules.filter(r => {
                const predNorm = normalizeStr(r.predecesora);
                // Usamos includes pero validamos que sea una palabra completa o un patrón reconocido
                return predNorm === groupTriggerNorm || predNorm.includes(groupTriggerNorm);
            });

            if (groupNext.length > 0) {
                candidates.push(...groupNext);
            }
        }
    });

    let results = [];
    candidates.forEach(c => {
        const cNorm = normalizeStr(c.seccion);
        if (allowedNorms.includes(cNorm)) {
            // Éxito: Encontrada sección válida
            results.push(c);
        } else {
            // Salto: Esta sección no le toca al producto, seguimos buscando recursivo
            const deeper = findNextAllowedSections(cNorm, flowRules, allowedNorms, visited);
            results.push(...deeper);
        }
    });

    // Eliminar duplicados por nombre de sección
    const unique = [];
    const seen = new Set();
    for (const item of results) {
        const n = normalizeStr(item.seccion);
        if (!seen.has(n)) {
            seen.add(n);
            unique.push(item);
        }
    }


    return unique;
}

/**
 * Calcula la ruta completa de secciones para un producto según el flujo y secciones permitidas.
 */
function getFullProductPath(productCode) {
    const flowType = (prodFlowTypeMap[productCode] || '').toLowerCase();
    const allowedSections = prodSectionsMap[productCode] || new Set();
    const flowRules = flowType.includes('pintura') ? flowsCache.pintura : flowsCache.tapiceria;
    const allowedNorms = Array.from(allowedSections).map(s => normalizeStr(s));

    if (!flowRules || flowRules.length === 0) return "N/A (Sin reglas de flujo)";

    // Encontrar la sección inicial (aquella que no tiene predecesora o cuya predecesora no está en el flujo)
    // O simplemente empezar desde las que tienen predecesora vacía en las reglas
    let currentSections = flowRules.filter(r => !r.predecesora || r.predecesora.trim() === "");

    // Filtrar solo las que le tocan al producto
    let fullPath = [];
    let visited = new Set();

    function trace(currentSecNorm) {
        if (visited.has(currentSecNorm)) return;
        visited.add(currentSecNorm);

        const sectionName = Array.from(allowedSections).find(s => normalizeStr(s) === currentSecNorm);
        if (sectionName) {
            fullPath.push(sectionName);
        }

        const nextOptions = findNextAllowedSections(currentSecNorm, flowRules, allowedNorms);
        nextOptions.forEach(opt => trace(normalizeStr(opt.seccion)));
    }

    // Iniciamos el rastro desde las raíces
    currentSections.forEach(root => trace(normalizeStr(root.seccion)));

    return fullPath.length > 0 ? fullPath.join(" -> ") : "No se encontró ruta válida";
}

document.addEventListener('DOMContentLoaded', () => {
    initApp();
});

async function initApp() {
    try {
        supabaseClient = window.supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
        console.log('MDP App: Conexión con Supabase lista.');

        setupLogin();
        setupFilters();
        setupRollbackForm();
        setupGlobalMassFill();
        checkExistingSession();
    } catch (err) {
        console.error('Error crítico al iniciar:', err);
    }
}

function setupLogin() {
    const loginForm = document.getElementById('login-form');
    const loginError = document.getElementById('login-error');
    const loginBtn = document.getElementById('login-btn');

    if (!loginForm) return;

    loginForm.addEventListener('submit', async (e) => {
        e.preventDefault();

        const username = document.getElementById('username').value.trim();
        const password = document.getElementById('password').value.trim();

        loginBtn.disabled = true;
        loginBtn.textContent = 'Verificando...';
        loginError.textContent = '';

        try {
            const { data, error } = await supabaseClient
                .from('usuarios')
                .select('id, nombre, usuario')
                .eq('usuario', username)
                .eq('contrasena', password)
                .single();

            if (error || !data) {
                throw new Error('Usuario o contraseña no válidos');
            }

            console.log('Usuario autenticado:', data);
            localStorage.setItem('mdp_session', JSON.stringify(data));
            loginSuccess(data);

        } catch (err) {
            console.error('Login error:', err);
            loginError.textContent = err.message;
            loginBtn.disabled = false;
            loginBtn.textContent = 'Entrar al Sistema';
        }
    });
}

// ---------------------------------------------------------
// DASHBOARD DE ADMINISTRACIÓN
// ---------------------------------------------------------

async function fetchMasterData(orderIds, productCodes) {
    if (orderIds.length === 0) return;

    try {
        const [resOrders, resProducts] = await Promise.all([
            supabaseClient.from('ordenes_produccion').select('*').in('id', orderIds),
            supabaseClient.from('productos').select('codigo, nombre').in('codigo', productCodes)
        ]);

        if (resOrders.data) {
            resOrders.data.forEach(o => {
                ordersMap[o.id] = {
                    product: o.producto,
                    OC: o.OC,
                    consecutivo: o.consecutivo,
                    displayId: `${o.OC || ''}-${o.consecutivo || o.id}`.replace(/^-|-$/, '')
                };
            });
        }

        if (resProducts.data) {
            resProducts.data.forEach(p => {
                productsMap[p.codigo] = p.nombre;
            });
        }
    } catch (err) {
        console.error('Error fetching master data:', err);
    }
}

async function loadAdminDashboard() {

    const historyBody = document.getElementById('admin-history-body');
    const tableLoader = document.getElementById('table-loader');

    try {
        tableLoader.style.display = 'block';
        historyBody.innerHTML = '';

        // 1. Obtener TODO el historial
        const { data: history, error } = await supabaseClient
            .from('historial_movimientos')
            .select('*')
            .order('fecha_movimiento', { ascending: false })
            .limit(500); // Límite razonable para no saturar

        if (error) throw error;

        adminHistoryData = history || [];

        // 2. Enriquecer con Datos Maestro (OCs y Nombres de Productos)
        const orderIds = [...new Set(adminHistoryData.map(h => h.id_orden))];
        const productCodes = [...new Set(adminHistoryData.map(h => h.producto))];
        await fetchMasterData(orderIds, productCodes);

        // 3. Poblar selector de productos de admin si está vacío
        const productSelect = document.getElementById('admin-filter-product');
        if (productSelect && productSelect.options.length <= 1) {
            const products = [...new Set(adminHistoryData.map(h => h.producto))].sort();
            products.forEach(p => {
                if (p) productSelect.innerHTML += `<option value="${p}">${p}</option>`;
            });
        }

        renderAdminHistory(adminHistoryData);

        // 3. Activar Eventos de Filtro Admin
        document.getElementById('admin-filter-op').oninput = applyAdminFilters;
        document.getElementById('admin-filter-product').onchange = applyAdminFilters;
        document.getElementById('admin-filter-worker').oninput = applyAdminFilters;
        document.getElementById('admin-filter-status').onchange = applyAdminFilters;
        document.getElementById('admin-filter-leader').oninput = applyAdminFilters;
        document.getElementById('admin-clear-filters').onclick = () => {
            document.getElementById('admin-filter-op').value = '';
            document.getElementById('admin-filter-product').value = '';
            document.getElementById('admin-filter-worker').value = '';
            document.getElementById('admin-filter-status').value = '';
            document.getElementById('admin-filter-leader').value = '';
            renderAdminHistory(adminHistoryData);
        };

    } catch (err) {
        console.error('Error cargando Dashboard Admin:', err);
    } finally {
        tableLoader.style.display = 'none';
    }
}

function renderAdminHistory(data) {
    const historyBody = document.getElementById('admin-history-body');
    historyBody.innerHTML = '';

    if (data.length === 0) {
        historyBody.innerHTML = '<tr><td colspan="6" style="text-align:center;">No se encontró historial con esos filtros.</td></tr>';
        return;
    }

    data.forEach(h => {
        const fecha = new Date(h.fecha_movimiento);
        const fechaStr = fecha.toLocaleDateString() + ' ' + fecha.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

        const orderInfo = ordersMap[h.id_orden] || { displayId: h.id_orden };
        const productName = productsMap[h.producto] || h.producto;

        // Badge de estatus
        let statusClass = 'status-normal';
        let statusText = 'En Producción';

        if (h.estatus_final === 'RETROCESO') {
            statusClass = 'status-retroceso';
            statusText = '⚠️ Retroceso';
        } else if (h.estatus_final === 'Despachado' || h.estatus_final === 'DESPACHADO') {
            statusClass = 'status-despachado';
            statusText = '✅ Despachado';
        }

        const tr = document.createElement('tr');
        tr.className = 'admin-history-item';
        tr.innerHTML = `
            <td>
                <span class="row-main-info">${fechaStr}</span>
                <span class="row-sub-info">ID: ${h.id}</span>
            </td>
            <td>
                <span class="row-main-info">OP: ${orderInfo.displayId}</span>
                <span class="row-sub-info">${productName}</span>
            </td>

            <td>
                <div class="movement-path">
                    <span>${h.seccion_origen || 'Inicio'}</span>
                    <span class="path-arrow">→</span>
                    <span>${h.seccion_destino}</span>
                </div>
                <span class="history-status-badge ${statusClass}">${statusText}</span>
            </td>
            <td><b>${h.cantidad_movida}</b> pzas</td>
            <td>
                <span class="row-main-info">${h.operarios || 'N/A'}</span>
                <span class="row-sub-info">Hora: ${h.hora_salida || '--:--'}</span>
                <span class="row-sub-info">Líder: <b>${h.lider_que_movio || '---'}</b></span>
            </td>
            <td>
                <div class="obs-text" title="${h.observaciones || ''}">${h.observaciones || '--'}</div>
            </td>
        `;
        historyBody.appendChild(tr);
    });
}

function applyAdminFilters() {
    const op = document.getElementById('admin-filter-op').value.toLowerCase();
    const product = document.getElementById('admin-filter-product').value;
    const worker = document.getElementById('admin-filter-worker').value.toLowerCase();
    const status = document.getElementById('admin-filter-status').value;
    const leader = document.getElementById('admin-filter-leader').value.toLowerCase();

    const filtered = adminHistoryData.filter(h => {
        const matchOp = !op || String(h.id_orden).toLowerCase().includes(op);
        const matchProduct = !product || h.producto === product;
        const matchWorker = !worker || (h.operarios && h.operarios.toLowerCase().includes(worker));
        const matchLeader = !leader || (h.lider_que_movio && h.lider_que_movio.toLowerCase().includes(leader));

        let matchStatus = true;
        if (status === 'RETROCESO') matchStatus = h.estatus_final === 'RETROCESO';
        else if (status === 'DESPACHADO') matchStatus = h.estatus_final === 'Despachado' || h.estatus_final === 'DESPACHADO';
        else if (status === 'PRODUCCION') matchStatus = h.estatus_final !== 'RETROCESO' && h.estatus_final !== 'Despachado' && h.estatus_final !== 'DESPACHADO';

        return matchOp && matchProduct && matchWorker && matchStatus && matchLeader;
    });

    renderAdminHistory(filtered);
}


function checkExistingSession() {
    const session = localStorage.getItem('mdp_session');
    if (session) {
        loginSuccess(JSON.parse(session));
    }
}

function loginSuccess(userData) {
    currentUserData = userData;
    document.getElementById('login-screen').style.display = 'none';
    const mainContainer = document.getElementById('main-container');
    mainContainer.style.display = 'block';

    document.getElementById('welcome-user').textContent = userData.nombre || userData.usuario;

    document.getElementById('logout-btn').onclick = () => {
        localStorage.removeItem('mdp_session');
        location.reload();
    };

    const saveBtn = document.getElementById('save-movements-btn');
    const tabsNav = document.querySelector('.tabs-nav');

    if (userData.usuario === 'admin') {
        // MODO ADMINISTRADOR
        document.getElementById('view-pending').style.display = 'none';
        document.getElementById('view-process').style.display = 'none';
        if (tabsNav) tabsNav.style.display = 'none';
        if (saveBtn) saveBtn.style.display = 'none';
        document.getElementById('filters-container').style.display = 'none';
        document.getElementById('view-admin').style.display = 'block';

        loadAdminDashboard();
    } else {
        // MODO LÍDER PLANTA
        document.getElementById('view-admin').style.display = 'none';
        if (tabsNav) tabsNav.style.display = 'flex';
        saveBtn.style.display = 'flex';
        saveBtn.onclick = () => saveAllMovements();
        loadProductionTracking(userData);
    }
}

function switchTab(tabId) {
    // Nav buttons
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    document.getElementById(`tab-btn-${tabId}`).classList.add('active');

    // Tab content
    document.querySelectorAll('.tab-content').forEach(view => view.classList.remove('active'));
    document.getElementById(`view-${tabId}`).classList.add('active');

    // Botón guardar solo visible en labor
    const saveBtn = document.getElementById('save-movements-btn');
    saveBtn.style.display = tabId === 'process' ? 'flex' : 'none';
}

async function loadProductionTracking(userData) {
    const tableLoader = document.getElementById('table-loader');
    const emptyState = document.getElementById('empty-state');
    const pendingBody = document.getElementById('pending-body');
    const processBody = document.getElementById('production-body');
    const plantInfo = document.getElementById('plant-info');
    const saveBtn = document.getElementById('save-movements-btn');
    const filtersContainer = document.getElementById('filters-container');

    if (!pendingBody || !processBody) return;

    try {
        tableLoader.style.display = 'block';
        emptyState.style.display = 'none';
        pendingBody.innerHTML = '';
        processBody.innerHTML = '';
        saveBtn.style.display = 'none';
        filtersContainer.style.display = 'none';

        // 1. Obtener planta del líder
        const { data: leaderRows, error: leaderError } = await supabaseClient
            .from('lideres_planta')
            .select(`id_planta, plantas (nombre)`)
            .eq('id_usuario', userData.id);

        if (leaderError) throw leaderError;

        if (!leaderRows || leaderRows.length === 0) {
            plantInfo.textContent = 'Sin planta asignada';
            tableLoader.style.display = 'none';
            emptyState.style.display = 'block';
            return;
        }

        const plantName = leaderRows[0].plantas.nombre;
        plantInfo.textContent = `Planta: ${plantName}`;

        // 2. Cargar Flujos si no están en caché
        if (flowsCache.pintura.length === 0) {
            const [pRes, tRes] = await Promise.all([
                supabaseClient.from('flujo_pintura').select('*').order('id'),
                supabaseClient.from('flujo_tapiceria').select('*').order('id')
            ]);
            flowsCache.pintura = pRes.data || [];
            flowsCache.tapiceria = tRes.data || [];
        }

        // 3. Obtener seguimiento
        const { data: prodData, error: prodError } = await supabaseClient
            .from('seguimiento_produccion')
            .select('*')
            .eq('planta', plantName)
            .neq('estatus', 'Despachado') // Ignorar los ya finalizados
            .order('created_at', { ascending: false });

        if (prodError) throw prodError;

        if (!prodData || prodData.length === 0) {
            tableLoader.style.display = 'none';
            emptyState.style.display = 'block';
            return;
        }

        allTrackingData = prodData;

        // 4. Enriquecer datos
        const orderIds = [...new Set(prodData.map(i => i.id_orden))];
        const { data: ordersData } = await supabaseClient
            .from('ordenes_produccion')
            .select('*')
            .in('id', orderIds);

        ordersMap = Object.fromEntries(
            ordersData?.map(o => [o.id, {
                product: o.producto,
                OC: o.OC,
                consecutivo: o.consecutivo,
                displayId: `${o.OC || ''}-${o.consecutivo || o.id}`.replace(/^-|-$/, '')
            }]) || []
        );

        const productCodes = [...new Set(Object.values(ordersMap).map(o => o.product))];

        const [resProducts, resProdFlowType, resProdSections, resPlantSections] = await Promise.all([
            supabaseClient.from('productos').select('codigo, nombre').in('codigo', productCodes),
            supabaseClient.from('productos_flujo').select('codigo, flujo').in('codigo', productCodes),
            supabaseClient.from('productos_secciones').select('codigo, seccion_planta').in('codigo', productCodes),
            supabaseClient.from('plantas_secciones').select('nombre_seccion, nombre_planta')
        ]);

        productsMap = Object.fromEntries(resProducts.data?.map(p => [p.codigo, p.nombre]) || []);
        prodFlowTypeMap = Object.fromEntries(resProdFlowType.data?.map(f => [f.codigo, f.flujo]) || []);
        sectionToPlantMap = Object.fromEntries(resPlantSections.data?.map(ps => [ps.nombre_seccion, ps.nombre_planta]) || []);

        prodSectionsMap = {};
        resProdSections.data?.forEach(ps => {
            if (!prodSectionsMap[ps.codigo]) prodSectionsMap[ps.codigo] = new Set();
            prodSectionsMap[ps.codigo].add(ps.seccion_planta);
        });

        // LOG DE RUTAS DE PRODUCTOS
        console.group('%c RUTAS DE SECCIONES POR PRODUCTO ', 'background: #222; color: #bada55; font-size: 12px;');
        productCodes.forEach(code => {
            const path = getFullProductPath(code);
            console.log(`%c Producto: ${code} (${productsMap[code] || '?'}) %c Ruta: ${path} `, 'font-weight: bold; color: #4CAF50', 'color: #2196F3');
        });
        console.groupEnd();

        // 5. Preparar Filtros y Renderizar
        updateFilterOptions();
        filtersContainer.style.display = 'flex';
        tableLoader.style.display = 'none';
        saveBtn.style.display = 'flex';

        applyFiltersAndRender();

    } catch (err) {
        console.error('Error dashboard:', err);
        tableLoader.innerHTML = `<p class="error-msg">Error: ${err.message}</p>`;
    }
}

function setupFilters() {
    const filters = ['filter-op', 'filter-product', 'filter-section', 'filter-flow'];
    filters.forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            const event = el.tagName === 'SELECT' ? 'change' : 'input';
            el.addEventListener(event, applyFiltersAndRender);
        }
    });

    document.getElementById('clear-filters-btn')?.addEventListener('click', () => {
        filters.forEach(id => {
            const el = document.getElementById(id);
            if (el) el.value = '';
        });
        applyFiltersAndRender();
    });
}

function updateFilterOptions() {
    const productSelect = document.getElementById('filter-product');
    const sectionSelect = document.getElementById('filter-section');
    if (!productSelect || !sectionSelect) return;

    // Productos únicos en los datos actuales
    const productCodes = [...new Set(allTrackingData.map(i => ordersMap[i.id_orden]?.product).filter(Boolean))];
    productSelect.innerHTML = '<option value="">Todos los productos</option>' +
        productCodes.map(code => `<option value="${code}">${productsMap[code] || code}</option>`).join('');

    // Secciones únicas en los datos actuales
    const sections = [...new Set(allTrackingData.map(i => i.seccion_planta).filter(Boolean))].sort();
    sectionSelect.innerHTML = '<option value="">Todas las secciones</option>' +
        sections.map(s => `<option value="${s}">${s}</option>`).join('');
}

function applyFiltersAndRender() {
    const opVal = normalizeStr(document.getElementById('filter-op')?.value || '');
    const productVal = document.getElementById('filter-product')?.value || '';
    const sectionVal = document.getElementById('filter-section')?.value || '';
    const flowVal = document.getElementById('filter-flow')?.value || '';

    const filtered = allTrackingData.filter(item => {
        const orderInfo = ordersMap[item.id_orden] || {};
        const flowType = (prodFlowTypeMap[orderInfo.product] || '').toLowerCase();

        const matchOp = !opVal || normalizeStr(orderInfo.displayId).includes(opVal) || normalizeStr(orderInfo.OC || '').includes(opVal);
        const matchProduct = !productVal || orderInfo.product === productVal;
        const matchSection = !sectionVal || item.seccion_planta === sectionVal;
        const matchFlow = !flowVal || flowType.includes(flowVal);

        return matchOp && matchProduct && matchSection && matchFlow;
    });

    renderCurrentTables(filtered);
}

function renderCurrentTables(data) {
    const pendingBody = document.getElementById('pending-body');
    const processBody = document.getElementById('production-body');
    const emptyState = document.getElementById('empty-state');

    const pendingItems = data.filter(i => i.estatus === 'Pendiente');
    const activeItems = data.filter(i => i.estatus === 'En Proceso');

    pendingBody.innerHTML = pendingItems.map(item => {
        const orderInfo = ordersMap[item.id_orden] || { product: '---', displayId: item.id_orden };
        const productName = productsMap[orderInfo.product] || orderInfo.product;
        return `
            <tr>
                <td><strong>${orderInfo.displayId}</strong> </td>
                <td>${item.codigo_seccion || '--'} <br><small>${item.seccion_planta || '--'}</small></td>
                <td>${item.cantidades || 0}</td>
                <td><small><b>${orderInfo.product}</b> <br> ${productName}</small></td>
                <td>
                    <button class="btn-primary" style="margin:0; padding: 0.5rem 1rem; font-size: 0.8rem;" onclick="startProduction('${item.id}')">Iniciar Producción</button>
                </td>
            </tr>
        `;
    }).join('');

    processBody.innerHTML = activeItems.map(item => {
        const orderInfo = ordersMap[item.id_orden] || { product: '---', displayId: item.id_orden };
        const productName = productsMap[orderInfo.product] || orderInfo.product;
        const flowType = (prodFlowTypeMap[orderInfo.product] || '').toLowerCase();
        const allowedSections = prodSectionsMap[orderInfo.product] || new Set();
        const flowRules = flowType.includes('pintura') ? flowsCache.pintura : flowsCache.tapiceria;

        const currentSecNorm = normalizeStr(item.seccion_planta);
        const allowedNorms = Array.from(allowedSections).map(s => normalizeStr(s));
        const finalOptions = findNextAllowedSections(currentSecNorm, flowRules, allowedNorms);

        const optionsHtml = finalOptions.length > 0
            ? finalOptions.map(o => {
                const targetPlant = sectionToPlantMap[o.seccion] || "";
                const isPlantChange = targetPlant && targetPlant !== item.planta;
                const displayLabel = isPlantChange ? `FINALIZAR EN ESTA PLANTA (Hacia: ${targetPlant})` : o.seccion;
                return `<option value="${o.seccion}" data-is-final="${o.final_seccion}" data-target-plant="${targetPlant}">${displayLabel}</option>`;
            }).join('')
            : '<option value="FIN_PROCESO">Fin del proceso</option>';

        const currentQty = item.cantidades || 0;

        return `
            <tr data-row-id="${item.id}" 
                data-order-id="${item.id_orden}" 
                data-product="${orderInfo.product}"
                data-current-sec-planta="${item.seccion_planta}" 
                data-current-plant="${item.planta}"
                data-original-qty="${currentQty}"
                data-full-item='${JSON.stringify(item)}'>
                <td><strong>${orderInfo.displayId}</strong> <br> <br> <span class="badge badge-status">${flowType.toUpperCase() || 'N/A'}</span></td>
                <td>${item.codigo_seccion || '--'} <br><small>${item.seccion_planta || '--'}</small></td>
                <td>${currentQty}</td>
                <td style="max-width: 200px; overflow: hidden; word-wrap: break-word;"><small>${orderInfo.product} <br> ${productName}</small></td>
                <td>
                    <select class="table-input table-select next-sec">
                        <option value="">Seleccione...</option>
                        ${optionsHtml}
                    </select>
                </td>
                <td>
                    <input type="number" class="table-input next-qty" value="${currentQty}" min="1" max="${currentQty}" onchange="validateQty(this, ${currentQty})">
                </td>
                <td>
                    <input type="number" class="table-input num-workers" value="1" min="1">
                </td>
                <td>
                    <input type="time" class="table-input exit-time" value="${new Date().toTimeString().slice(0, 5)}">
                </td>
                <td>
                    <input type="text" class="table-input observaciones" placeholder="...">
                </td>
                <td>
                    <button class="btn-rollback" title="Retroceder a sección anterior" onclick="openRollbackModal('${item.id}')">
                        ⚠️
                    </button>
                </td>
            </tr>
        `;
    }).join('');

    emptyState.style.display = (pendingItems.length === 0 && activeItems.length === 0) ? 'block' : 'none';
}

async function startProduction(id) {
    try {
        const { error } = await supabaseClient
            .from('seguimiento_produccion')
            .update({ estatus: 'En Proceso' })
            .eq('id', id);

        if (error) throw error;

        loadProductionTracking(currentUserData);
        alert('Producción iniciada para este registro.');
    } catch (err) {
        console.error('Error al iniciar producción:', err);
        alert('Error: ' + err.message);
    }
}

function validateQty(input, max) {
    let val = parseInt(input.value);
    if (isNaN(val) || val < 1) input.value = 1;
    if (val > max) input.value = max;
}

async function saveAllMovements() {
    const rows = document.querySelectorAll('#production-body tr');
    const movements = [];

    // 1. Recolectar datos de todas las filas con movimiento
    rows.forEach(row => {
        const selectEl = row.querySelector('.next-sec');
        const selectOption = selectEl.options[selectEl.selectedIndex];
        const nextSec = selectEl.value;
        if (!nextSec) return;

        movements.push({
            rowId: row.dataset.rowId,
            orderId: row.dataset.orderId,
            product: row.dataset.product,
            currentSecPlanta: row.dataset.currentSecPlanta,
            currentPlant: row.dataset.currentPlant,
            nextSec: nextSec,
            isFinal: selectOption?.dataset.isFinal === 'true',
            targetPlant: selectOption?.dataset.targetPlant,
            nextQty: parseInt(row.querySelector('.next-qty').value),
            originalQty: parseInt(row.dataset.originalQty),
            workers: row.querySelector('.num-workers').value,
            exitTime: row.querySelector('.exit-time').value,
            obs: row.querySelector('.observaciones').value,
            fullItem: JSON.parse(row.dataset.fullItem)
        });
    });

    if (movements.length === 0) return alert('No hay movimientos seleccionados.');

    // 2. Agrupar por (ID Orden + Sección Destino) para detectar uniones (Ensamble)
    const groups = {};
    movements.forEach(m => {
        const key = `${m.orderId}|${m.nextSec}`;
        if (!groups[key]) groups[key] = [];
        groups[key].push(m);
    });

    const historyEntries = [];
    const trackingUpdates = [];
    const trackingInserts = [];
    const trackingDeletions = [];
    const movedToOtherPlant = [];
    let plantChangeDetected = false;

    // 3. Procesar cada grupo (normal o unión)
    for (const key in groups) {
        const group = groups[key];
        const isMerge = group.length > 1;

        let finalMergeQty = group[0].nextQty;

        if (isMerge) {
            const maxAllowed = Math.max(...group.map(m => m.nextQty));
            const input = prompt(`Unión detectada en Orden ${group[0].orderId}.\nIngrese la CANTIDAD FINAL para el ensamble en "${group[0].nextSec}":\n(Rango permitido: 1 - ${maxAllowed})`, maxAllowed);

            if (input === null) return; // Usuario canceló el prompt

            const parsedInput = parseInt(input);
            if (isNaN(parsedInput) || parsedInput <= 0 || parsedInput > maxAllowed) {
                alert(`Cantidad inválida. Debe ser un número entre 1 y ${maxAllowed}.`);
                return;
            }
            finalMergeQty = parsedInput;
        }

        // El primer elemento del grupo será el "portador" (el que se actualiza o inserta)
        const carrier = group[0];

        group.forEach((m, idx) => {
            // Registrar historial para CADA componente (trazabilidad)
            const isFinalStep = m.nextSec === 'FIN_PROCESO';

            historyEntries.push({
                id_seguimiento: m.rowId,
                id_orden: m.orderId,
                producto: m.product,
                seccion_origen: m.currentSecPlanta,
                seccion_destino: isFinalStep ? 'Fin del proceso' : m.nextSec,
                cantidad_movida: m.nextQty, // Lo que "sale" de esta sección
                estatus_final: isFinalStep ? 'DESPACHADO' : (m.isFinal ? 'FINALIZADO' : 'En Proceso'),
                observaciones: m.obs + (isMerge ? ` (Unión de partes - Cant. Ensamble: ${finalMergeQty})` : ''),
                operarios: m.workers,
                hora_salida: m.exitTime,
                lider_que_movio: currentUserData.nombre || currentUserData.usuario,
                fecha_movimiento: new Date().toISOString()
            });

            if (m.targetPlant && m.targetPlant !== m.currentPlant) {
                plantChangeDetected = true;
                const orderInfo = ordersMap[m.orderId] || {};
                const prodName = productsMap[m.product] || m.product;
                movedToOtherPlant.push(`• OP: ${orderInfo.displayId} - ${prodName} (Hacia: ${m.targetPlant})`);
            }

            const remainder = m.originalQty - m.nextQty;

            if (idx === 0) {
                // El primer elemento (carrier) se encarga de representar el resultado en la nueva sección
                if (remainder === 0) {
                    // SE REUSA EL REGISTRO: Se mueve el registro original al destino con la cantidad manual final
                    const updateObj = {
                        id: m.rowId,
                        seccion_planta: isFinalStep ? m.currentSecPlanta : m.nextSec,
                        cantidades: finalMergeQty,
                        estatus: isFinalStep ? 'Despachado' : 'En Proceso',
                        created_at: new Date().toISOString()
                    };

                    if (!isFinalStep && m.targetPlant && m.targetPlant !== m.currentPlant) {
                        updateObj.planta = m.targetPlant;
                        updateObj.estatus = 'Pendiente';
                    }
                    trackingUpdates.push(updateObj);
                } else {
                    // SE DIVIDE: El original se queda con el remanente (>0), y se CREA uno nuevo para el destino
                    trackingUpdates.push({
                        id: m.rowId,
                        cantidades: remainder
                    });

                    const newItem = { ...m.fullItem };
                    delete newItem.id;
                    newItem.seccion_planta = isFinalStep ? m.currentSecPlanta : m.nextSec;
                    newItem.cantidades = finalMergeQty;
                    newItem.estatus = isFinalStep ? 'Despachado' : 'En Proceso';
                    newItem.created_at = new Date().toISOString();
                    if (!isFinalStep && m.targetPlant && m.targetPlant !== m.currentPlant) {
                        newItem.planta = m.targetPlant;
                        newItem.estatus = 'Pendiente';
                    }
                    trackingInserts.push(newItem);
                }
            } else {
                // Miembros adicionales del grupo de unión
                if (remainder === 0) {
                    // Se consumió todo, se borra el registro de seguimiento
                    trackingDeletions.push(m.rowId);
                } else {
                    // Quedó algo, se actualiza el original con el sobrante
                    trackingUpdates.push({
                        id: m.rowId,
                        cantidades: remainder
                    });
                }
            }
        });
    }

    const btn = document.getElementById('save-movements-btn');
    btn.disabled = true;
    btn.innerText = 'Guardando...';

    try {
        // 1. Registrar historial (Insert masivo)
        const { error: histErr } = await supabaseClient.from('historial_movimientos').insert(historyEntries);
        if (histErr) throw histErr;

        // 2. Ejecutar actualizaciones (Individuales vía Promise.all para evitar errores de columnas en upsert)
        if (trackingUpdates.length > 0) {
            const updatePromises = trackingUpdates.map(u =>
                supabaseClient.from('seguimiento_produccion').update(u).eq('id', u.id)
            );
            const results = await Promise.all(updatePromises);
            const firstErr = results.find(r => r.error);
            if (firstErr) throw firstErr.error;
        }

        if (trackingInserts.length > 0) {
            const { error: inErr } = await supabaseClient.from('seguimiento_produccion').insert(trackingInserts);
            if (inErr) throw inErr;
        }

        if (trackingDeletions.length > 0) {
            const { error: delErr } = await supabaseClient.from('seguimiento_produccion').delete().in('id', trackingDeletions);
            if (delErr) throw delErr;
        }

        if (plantChangeDetected) {
            const list = movedToOtherPlant.join('\n');
            alert(`¡Movimientos registrados!\n\nSe ha terminado la producción en esta planta para:\n${list}`);
        } else {
            alert('¡Movimientos registrados con éxito!');
        }
        loadProductionTracking(currentUserData);

        btn.disabled = false;
        btn.innerText = '💾 Guardar Movimientos';

    } catch (err) {
        console.error('Error al guardar:', err);
        alert('Error: ' + err.message);
        btn.disabled = false;
        btn.innerText = '💾 Guardar Movimientos';
    }
}

// --- FUNCIONALIDAD DE RETROCESO ---

function setupRollbackForm() {
    const form = document.getElementById('rollback-form');
    if (form) {
        form.onsubmit = async (e) => {
            e.preventDefault();
            await submitRollback();
        };
    }
}

let activeRollbackItem = null;
let adminHistoryData = []; // Para la vista de administrador


async function openRollbackModal(itemId) {
    console.log('[Retroceso] Iniciando para:', itemId);
    const modal = document.getElementById('modal-rollback');
    const info = document.getElementById('rollback-info');
    const targetSelect = document.getElementById('rollback-target-sec');
    const reasonText = document.getElementById('rollback-reason');

    if (!modal) {
        console.error('[Retroceso] CRÍTICO: No se encontró el elemento modal-rollback en el DOM');
        return;
    }

    const item = allTrackingData.find(i => i.id == itemId);
    if (!item) {
        console.error('[Retroceso] No se encontró el item en allTrackingData. Data actual:', allTrackingData);
        return;
    }

    activeRollbackItem = item;
    const orderInfo = ordersMap[item.id_orden] || {};

    info.innerHTML = `Orden: <b>${orderInfo.displayId}</b> | Producto: <b>${orderInfo.product}</b><br>Sección Actual: <b>${item.seccion_planta}</b>`;
    reasonText.value = '';
    const workersInput = document.getElementById('rollback-workers');
    const exitTimeInput = document.getElementById('rollback-exit-time');
    if (workersInput) workersInput.value = '1';
    if (exitTimeInput) exitTimeInput.value = new Date().toTimeString().slice(0, 5);

    // LÓGICA SIMPLIFICADA: Mostrar todas las secciones permitidas del producto
    const productCode = orderInfo.product;
    const allowedSections = Array.from(prodSectionsMap[productCode] || []);

    // Filtrar la sección actual para no mostrarla en el retroceso
    const previousSections = allowedSections
        .filter(s => normalizeStr(s) !== normalizeStr(item.seccion_planta))
        .sort();

    console.log('[Retroceso] Secciones permitidas para el producto:', previousSections);

    if (previousSections.length === 0) {
        targetSelect.innerHTML = '<option value="">No hay secciones disponibles</option>';
    } else {
        targetSelect.innerHTML = '<option value="">Seleccione sección destino...</option>' +
            previousSections.map(s => `<option value="${s}">${s}</option>`).join('');
    }

    modal.style.display = 'flex';
}

function closeRollbackModal() {
    const modal = document.getElementById('modal-rollback');
    if (modal) modal.style.display = 'none';
    activeRollbackItem = null;
}


async function submitRollback() {
    if (!activeRollbackItem) return;

    const targetSec = document.getElementById('rollback-target-sec').value;
    const reason = document.getElementById('rollback-reason').value.trim();
    const workers = document.getElementById('rollback-workers')?.value;
    const exitTime = document.getElementById('rollback-exit-time')?.value;

    if (!targetSec || !reason || !workers || !exitTime) {
        alert('Por favor complete todos los campos.');
        return;
    }

    const btn = document.querySelector('#rollback-form button[type="submit"]');
    if (btn) {
        btn.disabled = true;
        btn.innerText = 'Procesando...';
    }

    try {
        const timestamp = new Date().toISOString();

        // 1. Registrar Historial de Retroceso
        const historyEntry = {
            id_seguimiento: activeRollbackItem.id,
            id_orden: activeRollbackItem.id_orden,
            producto: ordersMap[activeRollbackItem.id_orden]?.product,
            seccion_origen: activeRollbackItem.seccion_planta,
            seccion_destino: targetSec,
            cantidad_movida: activeRollbackItem.cantidades,
            estatus_final: 'RETROCESO',
            observaciones: `RETROCESO: ${reason}`,
            operarios: workers,
            hora_salida: exitTime,
            lider_que_movio: currentUserData.nombre || currentUserData.usuario,
            fecha_movimiento: timestamp
        };

        const { error: histErr } = await supabaseClient.from('historial_movimientos').insert(historyEntry);
        if (histErr) throw histErr;

        // 2. Actualizar Tabla de Seguimiento
        // Determinamos si el retroceso implica cambio de planta (vía mapa)
        const targetPlant = sectionToPlantMap[targetSec] || activeRollbackItem.planta;
        const isPlantChange = targetPlant !== activeRollbackItem.planta;

        const updateObj = {
            seccion_planta: targetSec,
            planta: targetPlant,
            estatus: isPlantChange ? 'Pendiente' : 'En Proceso',
            created_at: timestamp
        };

        const { error: updErr } = await supabaseClient
            .from('seguimiento_produccion')
            .update(updateObj)
            .eq('id', activeRollbackItem.id);

        if (updErr) throw updErr;

        if (isPlantChange) {
            alert(`¡Retroceso registrado!\n\nSe ha terminado la producción en esta planta para:\n• OP: ${ordersMap[activeRollbackItem.id_orden]?.displayId} - ${productsMap[ordersMap[activeRollbackItem.id_orden]?.product] || ordersMap[activeRollbackItem.id_orden]?.product} (Hacia: ${targetPlant})`);
        } else {
            alert('El proceso ha sido retrocedido con éxito.');
        }
        closeRollbackModal();
        loadProductionTracking(currentUserData);

    } catch (err) {
        console.error('Error en rollback:', err);
        alert('Error: ' + err.message);
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.innerText = 'Confirmar Retroceso';
        }
    }
}

function setupGlobalMassFill() {
    const gWorkers = document.getElementById('global-workers');
    const gExitTime = document.getElementById('global-exit-time');

    if (!gWorkers || !gExitTime) return;

    // Seteamos hora actual por defecto si está vacío
    if (!gExitTime.value) {
        gExitTime.value = new Date().toTimeString().slice(0, 5);
    }

    const applyBatch = () => {
        const rows = document.querySelectorAll('#production-body tr');
        rows.forEach(row => {
            const nextSec = row.querySelector('.next-sec')?.value;
            if (nextSec && nextSec !== "") {
                const rowWorkers = row.querySelector('.num-workers');
                const rowExitTime = row.querySelector('.exit-time');

                if (rowWorkers) rowWorkers.value = gWorkers.value;
                if (rowExitTime) rowExitTime.value = gExitTime.value;
            }
        });
    };

    gWorkers.addEventListener('input', applyBatch);
    gExitTime.addEventListener('input', applyBatch);

    // También re-aplicar cuando se cambie una sección individual
    document.addEventListener('change', (e) => {
        if (e.target.classList.contains('next-sec')) {
            const val = e.target.value;
            if (val && val !== "") {
                const row = e.target.closest('tr');
                const rowWorkers = row.querySelector('.num-workers');
                const rowExitTime = row.querySelector('.exit-time');
                if (rowWorkers) rowWorkers.value = gWorkers.value;
                if (rowExitTime) rowExitTime.value = gExitTime.value;
            }
        }
    });
}

// HACER GLOBAL PARA ONCLICK
console.log('Exponiendo funciones globales...');
window.switchTab = switchTab;
window.startProduction = startProduction;
window.openRollbackModal = openRollbackModal;
window.closeRollbackModal = closeRollbackModal;
