// Loads the two ANI open-data CSVs into MongoDB (database "peajes_ani").
//
//   peajes          one document per toll station, tariffs EMBEDDED as an array
//                   of variable length (each toll charges its own categories)
//   trafico_mensual one document per toll x month, REFERENCING the toll by
//                   peaje_id, with per-category traffic embedded
//
// Run inside the mongodb container:
//   docker exec mongodb mongosh --quiet /mongo/load_mongo.js
// Idempotent: drops and recreates both collections on every run.

const fs = require("fs");

const CSV_DIR = "/csv";
const TARIFAS_CSV = `${CSV_DIR}/peajes_ani.csv`;
const TRAFICO_CSV = `${CSV_DIR}/trafico_ani.csv`;
const BATCH_SIZE = 1000;

// Minimal CSV parser: handles quoted fields, escaped quotes ("") and CRLF.
function parseCsv(path) {
  const text = fs.readFileSync(path, "utf8").replace(/^﻿/, "");
  const rows = [];
  let row = [], field = "", quoted = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quoted) {
      if (c === '"' && text[i + 1] === '"') { field += '"'; i++; }
      else if (c === '"') quoted = false;
      else field += c;
    } else if (c === '"') quoted = true;
    else if (c === ",") { row.push(field); field = ""; }
    else if (c === "\n") { row.push(field); rows.push(row); row = []; field = ""; }
    else if (c !== "\r") field += c;
  }
  if (field !== "" || row.length) { row.push(field); rows.push(row); }
  const header = rows.shift().map((h) => h.trim());
  return rows
    .filter((r) => r.length === header.length)
    .map((r) => Object.fromEntries(header.map((h, i) => [h, r[i].trim()])));
}

const toNumber = (v) => (v === "" || v == null ? null : Number(v));
const toDate = (v) => {
  if (!v) return null;
  const d = new Date(/[zZ]|[+-]\d\d:?\d\d$/.test(v) ? v : `${v}Z`); // no TZ in source -> UTC
  return isNaN(d) ? null : d;
};
const norm = (s) => (s || "").trim().toUpperCase();

// --- peajes: group tariff rows by toll ---------------------------------------
function buildPeajes(rows) {
  const byToll = new Map();
  for (const r of rows) {
    const id = toNumber(r.idpeaje);
    if (id === null) continue;
    if (!byToll.has(id)) {
      byToll.set(id, {
        _id: id,
        nombre: norm(r.peaje),
        ultimo_cambio_tarifa: toDate(r.ultimofechacambiopeaje),
        tarifas: [],
      });
    }
    byToll.get(id).tarifas.push({
      categoria: norm(r.idcategoriatarifa),
      valor: toNumber(r.valor),
    });
  }
  return [...byToll.values()].map((p) => ({ ...p, num_categorias: p.tarifas.length }));
}

// --- trafico_mensual: group traffic rows by toll x month ---------------------
function buildTrafico(rows) {
  const byKey = new Map();
  for (const r of rows) {
    const id = toNumber(r.idpeaje);
    const desde = toDate(r.desde);
    if (id === null || !desde) continue;
    const periodo = new Date(Date.UTC(desde.getUTCFullYear(), desde.getUTCMonth(), 1));
    const key = `${id}-${periodo.toISOString().slice(0, 7)}`;
    if (!byKey.has(key)) {
      byKey.set(key, {
        _id: key,
        peaje_id: id,
        periodo,
        anio: periodo.getUTCFullYear(),
        mes: periodo.getUTCMonth() + 1,
        categorias: [],
      });
    }
    const trafico = toNumber(r.cantidadtrafico) || 0;
    const tarifa = toNumber(r.valortarifa) || 0;
    byKey.get(key).categorias.push({
      categoria: norm(r.categoriatarifa),
      tarifa,
      trafico,
      evasores: toNumber(r.cantidadevasores) || 0,
      exentos: toNumber(r.cantidadexentos787) || 0,
    });
  }
  return [...byKey.values()].map((d) => {
    const sum = (f) => d.categorias.reduce((acc, c) => acc + f(c), 0);
    return {
      ...d,
      totales: {
        trafico: sum((c) => c.trafico),
        evasores: sum((c) => c.evasores),
        exentos: sum((c) => c.exentos),
        recaudo_teorico: sum((c) => c.trafico * c.tarifa),
      },
    };
  });
}

function insertInBatches(coll, docs) {
  for (let i = 0; i < docs.length; i += BATCH_SIZE) {
    coll.insertMany(docs.slice(i, i + BATCH_SIZE), { ordered: false });
  }
}

// --- main ---------------------------------------------------------------------
if (typeof db !== "undefined" && db.getSiblingDB) {
  const target = db.getSiblingDB("peajes_ani");

  const peajes = buildPeajes(parseCsv(TARIFAS_CSV));
  const trafico = buildTrafico(parseCsv(TRAFICO_CSV));

  target.peajes.drop();
  target.trafico_mensual.drop();
  insertInBatches(target.peajes, peajes);
  insertInBatches(target.trafico_mensual, trafico);

  target.peajes.createIndex({ nombre: 1 });
  target.peajes.createIndex({ "tarifas.categoria": 1 });
  target.trafico_mensual.createIndex({ peaje_id: 1, periodo: 1 });
  target.trafico_mensual.createIndex({ anio: 1 });

  print(`peajes:          ${target.peajes.countDocuments()} documentos`);
  print(`trafico_mensual: ${target.trafico_mensual.countDocuments()} documentos`);
  print("Índices creados en peajes {nombre}, {tarifas.categoria} y trafico_mensual {peaje_id, periodo}, {anio}");
}

if (typeof module !== "undefined") module.exports = { parseCsv, buildPeajes, buildTrafico };