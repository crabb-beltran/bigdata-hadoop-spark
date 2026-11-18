// MongoDB queries over the ANI toll database (peajes_ani).
//   1.x  CRUD: insert, select, update, delete
//   2.x  Filters and operators
//   3.x  Joins with $lookup
//   4.x  Aggregations: count, sum, average
//
// Run everything:   docker exec mongodb mongosh --quiet /mongo/queries.js
// Run one section:  docker exec -e Q=2.3 mongodb mongosh --quiet /mongo/queries.js

const d = db.getSiblingDB("peajes_ani");
const ONLY = (typeof process !== "undefined" && process.env.Q) || "";

function run(id, title, fn) {
  if (ONLY && !id.startsWith(ONLY)) return;
  print(`\n===== ${id} · ${title} =====`);
  const out = fn();
  if (out !== undefined) printjson(out);
}

// ---------------------------------------------------------------------------
// 1. CRUD
// ---------------------------------------------------------------------------
run("1.1", "Inserción: un peaje nuevo (insertOne) y su tráfico (insertMany)", () => {
  d.peajes.deleteOne({ _id: 9001 });                       // keeps the script re-runnable
  d.trafico_mensual.deleteMany({ peaje_id: 9001 });
  const peaje = d.peajes.insertOne({
    _id: 9001,
    nombre: "PEAJE DEMO UNAD",
    ultimo_cambio_tarifa: new Date("2026-01-16T00:00:00Z"),
    tarifas: [
      { categoria: "I", valor: 12000 },
      { categoria: "II", valor: 15000 },
    ],
    num_categorias: 2,
  });
  const trafico = d.trafico_mensual.insertMany([
    { _id: "9001-2026-06", peaje_id: 9001, periodo: new Date("2026-06-01T00:00:00Z"), anio: 2026, mes: 6,
      categorias: [{ categoria: "I", tarifa: 12000, trafico: 50000, evasores: 120, exentos: 300 }],
      totales: { trafico: 50000, evasores: 120, exentos: 300, recaudo_teorico: 600000000 } },
    { _id: "9001-2026-07", peaje_id: 9001, periodo: new Date("2026-07-01T00:00:00Z"), anio: 2026, mes: 7,
      categorias: [{ categoria: "I", tarifa: 12000, trafico: 52000, evasores: 90, exentos: 310 }],
      totales: { trafico: 52000, evasores: 90, exentos: 310, recaudo_teorico: 624000000 } },
  ]);
  return { peaje, trafico };
});

run("1.2", "Selección: tráfico mensual de ANDES en 2025 (find + proyección + orden)", () =>
  d.trafico_mensual
    .find({ peaje_id: 16, anio: 2025 }, { _id: 0, mes: 1, "totales.trafico": 1, "totales.evasores": 1 })
    .sort({ mes: 1 })
    .limit(6)
    .toArray()
);

run("1.3", "Actualización: cambiar una tarifa (arrayFilters), agregar una categoría ($push) y etiquetar la pandemia (updateMany)", () => {
  // Two separate updates: MongoDB rejects $set and $push on the same array in one operation.
  const cambioTarifa = d.peajes.updateOne(
    { _id: 9001 },
    { $set: { "tarifas.$[t].valor": 12500 } },          // only the element with categoria I
    { arrayFilters: [{ "t.categoria": "I" }] }
  );
  const nuevaCategoria = d.peajes.updateOne(
    { _id: 9001 },
    { $push: { tarifas: { categoria: "III", valor: 30000 } }, $inc: { num_categorias: 1 } }
  );
  const pandemia = d.trafico_mensual.updateMany(
    { periodo: { $gte: new Date("2020-03-01T00:00:00Z"), $lt: new Date("2021-01-01T00:00:00Z") } },
    { $set: { etiqueta: "pandemia" } }
  );
  return {
    cambioTarifa,
    nuevaCategoria,
    pandemia,
    resultado: d.peajes.findOne({ _id: 9001 }, { tarifas: 1, num_categorias: 1 }),
  };
});

run("1.4", "Eliminación: el peaje de prueba y su tráfico (deleteMany + deleteOne)", () => ({
  trafico: d.trafico_mensual.deleteMany({ peaje_id: 9001 }),
  peaje: d.peajes.deleteOne({ _id: 9001 }),
}));

// ---------------------------------------------------------------------------
// 2. Filters and operators
// ---------------------------------------------------------------------------
run("2.1", "Comparación ($gte, $lte): meses con más de 1 millón de vehículos desde 2024", () => ({
  total: d.trafico_mensual.countDocuments({ anio: { $gte: 2024 }, "totales.trafico": { $gte: 1000000 } }),
  ejemplos: d.trafico_mensual
    .find({ anio: { $gte: 2024 }, "totales.trafico": { $gte: 1000000 } }, { _id: 1, "totales.trafico": 1 })
    .sort({ "totales.trafico": -1 })
    .limit(5)
    .toArray(),
}));

run("2.2", "Lógicos ($or) y existencia ($exists): peajes con muchas categorías o alguna tarifa > 100.000", () =>
  d.peajes
    .find(
      { $or: [{ num_categorias: { $gte: 12 } }, { "tarifas.valor": { $gt: 100000 } }],
        ultimo_cambio_tarifa: { $exists: true } },
      { _id: 0, nombre: 1, num_categorias: 1 }
    )
    .sort({ num_categorias: -1 })
    .limit(5)
    .toArray()
);

run("2.3", "Arreglos ($elemMatch): meses en que la categoría I tuvo más de 500 evasores", () => ({
  total: d.trafico_mensual.countDocuments({ categorias: { $elemMatch: { categoria: "I", evasores: { $gt: 500 } } } }),
  ejemplos: d.trafico_mensual
    .find({ categorias: { $elemMatch: { categoria: "I", evasores: { $gt: 500 } } } },
          { _id: 1, "totales.evasores": 1 })
    .limit(5)
    .toArray(),
}));

run("2.4", "Pertenencia ($in) y expresión regular ($regex): peajes 'SAN…' con tarifa especial de categoría I", () =>
  d.peajes
    .find(
      { nombre: { $regex: "^SAN" }, "tarifas.categoria": { $in: ["IE", "IE1", "IE2"] } },
      { _id: 0, nombre: 1, num_categorias: 1 }
    )
    .toArray()
);

// ---------------------------------------------------------------------------
// 3. Joins with $lookup
// ---------------------------------------------------------------------------
run("3.1", "Unión ($lookup): top 5 peajes por tráfico en 2025, con nombre y tarifa de categoría I", () =>
  d.trafico_mensual.aggregate([
    { $match: { anio: 2025 } },
    { $group: { _id: "$peaje_id", trafico: { $sum: "$totales.trafico" } } },
    { $sort: { trafico: -1 } },
    { $limit: 5 },
    { $lookup: { from: "peajes", localField: "_id", foreignField: "_id", as: "peaje" } },
    { $unwind: { path: "$peaje", preserveNullAndEmptyArrays: true } },
    { $project: {
        _id: 0,
        peaje_id: "$_id",
        nombre: { $ifNull: ["$peaje.nombre", "(sin registro de tarifas)"] },
        trafico: 1,
        tarifa_I: { $first: {
          $map: { input: { $filter: { input: { $ifNull: ["$peaje.tarifas", []] }, cond: { $eq: ["$$this.categoria", "I"] } } },
                  in: "$$this.valor" } } },
    } },
  ]).toArray()
);

run("3.2", "Anti-join ($lookup + $size 0): peajes con tráfico pero sin registro de tarifas", () => {
  const sinTarifa = d.trafico_mensual.aggregate([
    { $group: { _id: "$peaje_id" } },
    { $lookup: { from: "peajes", localField: "_id", foreignField: "_id", as: "peaje" } },
    { $match: { peaje: { $size: 0 } } },
    { $sort: { _id: 1 } },
  ]).toArray();
  return { cantidad: sinTarifa.length, primeros_ids: sinTarifa.slice(0, 10).map((x) => x._id) };
});

// ---------------------------------------------------------------------------
// 4. Aggregations
// ---------------------------------------------------------------------------
run("4.1", "Contar ($count, $addToSet): documentos y peajes activos por año", () =>
  d.trafico_mensual.aggregate([
    { $group: { _id: "$anio", documentos: { $sum: 1 }, peajes: { $addToSet: "$peaje_id" } } },
    { $project: { _id: 0, anio: "$_id", documentos: 1, peajes_activos: { $size: "$peajes" } } },
    { $sort: { anio: 1 } },
  ]).toArray()
);

run("4.2", "Sumar ($sum): tráfico y recaudo teórico por año, con variación anual", () =>
  d.trafico_mensual.aggregate([
    { $group: { _id: "$anio", trafico: { $sum: "$totales.trafico" }, recaudo: { $sum: "$totales.recaudo_teorico" } } },
    { $sort: { _id: 1 } },
    { $setWindowFields: { sortBy: { _id: 1 }, output: { trafico_anterior: { $shift: { output: "$trafico", by: -1 } } } } },
    { $project: {
        _id: 0, anio: "$_id", trafico: 1,
        recaudo_billones: { $round: [{ $divide: ["$recaudo", 1e12] }, 2] },
        variacion_pct: { $cond: [{ $gt: ["$trafico_anterior", null] },
          { $round: [{ $multiply: [{ $divide: [{ $subtract: ["$trafico", "$trafico_anterior"] }, "$trafico_anterior"] }, 100] }, 1] },
          null] },
    } },
  ]).toArray()
);

run("4.3", "Promediar ($avg, $min, $max): tarifa vigente por categoría base ($unwind de tarifas)", () =>
  d.peajes.aggregate([
    { $unwind: "$tarifas" },
    { $match: { "tarifas.categoria": { $in: ["I", "II", "III", "IV", "V", "VI", "VII"] } } },
    { $group: { _id: "$tarifas.categoria", peajes: { $sum: 1 },
                promedio: { $avg: "$tarifas.valor" }, minimo: { $min: "$tarifas.valor" }, maximo: { $max: "$tarifas.valor" } } },
    { $project: { _id: 0, categoria: "$_id", peajes: 1, promedio: { $round: ["$promedio", 0] }, minimo: 1, maximo: 1 } },
    { $sort: { categoria: 1 } },
  ]).toArray()
);

run("4.4", "Tasa calculada + $lookup: top 5 peajes por evasores cada 1.000 vehículos (tráfico ≥ 1 M)", () =>
  d.trafico_mensual.aggregate([
    { $group: { _id: "$peaje_id", trafico: { $sum: "$totales.trafico" }, evasores: { $sum: "$totales.evasores" } } },
    { $match: { trafico: { $gte: 1000000 } } },
    { $project: { trafico: 1, evasores: 1, por_mil: { $round: [{ $multiply: [{ $divide: ["$evasores", "$trafico"] }, 1000] }, 2] } } },
    { $sort: { por_mil: -1 } },
    { $limit: 5 },
    { $lookup: { from: "peajes", localField: "_id", foreignField: "_id", as: "peaje" } },
    { $project: { _id: 0, peaje_id: "$_id", nombre: { $ifNull: [{ $first: "$peaje.nombre" }, "(sin registro de tarifas)"] },
                  trafico: 1, evasores: 1, por_mil: 1 } },
  ]).toArray()
);

run("4.5", "Optimización (explain): la selección 1.2 usa un índice en lugar de recorrer la colección", () => {
  const plan = d.trafico_mensual.find({ peaje_id: 16, anio: 2025 }).explain("executionStats");
  const stage = (s) => (s.inputStage ? stage(s.inputStage) : s);
  return {
    etapa: stage(plan.queryPlanner.winningPlan).stage,
    indice: stage(plan.queryPlanner.winningPlan).indexName,
    documentos_examinados: plan.executionStats.totalDocsExamined,
    documentos_devueltos: plan.executionStats.nReturned,
  };
});