/**
 * Netlify Function: chat
 * Endpoint: /.netlify/functions/chat
 * Handles SG Data Advisor AI chat using Google Generative Language API (gemma-4-31b-it)
 */

// ── Dataset knowledge base ────────────────────────────────────────────────────
const DATASETS = [
  { id: "bog_inflation",   name: "Bank of Ghana – Inflation & CPI Data",           sector: "Economy / Macroeconomics",       bestFor: ["inflation research","macroeconomic analysis","thesis on inflation","pricing studies","monetary policy"],        methods: ["Time series analysis","ARIMA forecasting","VAR models","Regression analysis"] },
  { id: "bog_exchange",    name: "Bank of Ghana – Exchange Rate Data",              sector: "Economy / Finance",              bestFor: ["exchange rate research","import/export analysis","forex studies","depreciation analysis","trade research"],      methods: ["Time series analysis","VAR models","GARCH models","Cointegration analysis"] },
  { id: "bog_interest",    name: "Bank of Ghana – Interest Rate Data",              sector: "Economy / Finance",              bestFor: ["monetary policy research","banking sector analysis","credit access studies","SME financing"],                   methods: ["Time series analysis","Regression","Panel data analysis","Cointegration"] },
  { id: "gss_sme",         name: "Ghana Statistical Service – SME Performance Data", sector: "SMEs / Business",              bestFor: ["SME research","entrepreneurship studies","K-means clustering","market segmentation","DSS research"],             methods: ["K-means clustering","Logistic regression","Decision trees","Descriptive analytics","PCA"] },
  { id: "gss_population",  name: "Ghana Statistical Service – Population & Housing Census", sector: "Demographics",           bestFor: ["demographic research","urban planning","public health","education access studies","poverty mapping"],            methods: ["Spatial analysis","Descriptive statistics","Regression","GIS mapping"] },
  { id: "gss_agriculture", name: "Ghana Statistical Service – Agriculture Survey Data", sector: "Agriculture",               bestFor: ["agriculture research","food security","crop yield forecasting","farmer segmentation","rural development"],        methods: ["Regression","K-means clustering","Time series","ANOVA","Random forest"] },
  { id: "gss_education",   name: "Ghana Statistical Service – Education Statistics",   sector: "Education",                  bestFor: ["education policy research","gender and education","AI in education","learning outcomes","educational access"],    methods: ["Regression","Descriptive analytics","Chi-square tests","Panel data","Logistic regression"] },
  { id: "wb_ghana",        name: "World Bank – Ghana Development Indicators",        sector: "Economy / Development",        bestFor: ["macroeconomic research","development economics","poverty analysis","comparative studies","FDI determinants"],     methods: ["Regression","Time series","Comparative analysis","VAR models","Cointegration"] },
  { id: "sgmpi",           name: "SG Market Price Index (SGMPI)",                   sector: "Pricing / Market Intelligence", bestFor: ["retail pricing","commodity pricing","business pricing strategy","SME pricing decisions","market intelligence"],  methods: ["Price elasticity analysis","Time series","Benchmarking","Regression"] },
  { id: "health_ghana",    name: "Ghana Health Service – Health Statistics",         sector: "Health",                       bestFor: ["public health research","healthcare access","disease burden analysis","maternal health","child health"],         methods: ["Regression","Survival analysis","Descriptive analytics","GIS mapping","Logistic regression"] },
  { id: "sustainability",  name: "Ghana Environmental & Sustainability Data",        sector: "Sustainability / Environment", bestFor: ["sustainability research","climate change studies","ESG analysis","green business","SDG research"],               methods: ["Time series","Regression","Correlation analysis","Scenario modelling"] },
];

function buildCatalog() {
  return DATASETS.map(d =>
    `[${d.id}] ${d.name} | Sector: ${d.sector}\n  Best for: ${d.bestFor.slice(0,4).join(", ")}\n  Methods: ${d.methods.join(", ")}\n  Buy: https://sgdatalytics.org/marketplace.html`
  ).join("\n\n");
}

const SYSTEM_PROMPT = `You are SG Data Advisor, an expert AI research and data consultant for SG Datalytics (https://sgdatalytics.org).
SG Datalytics is a Ghana-based data analytics company providing curated Ghana datasets and analytics services.

AVAILABLE GHANA DATASETS:
${buildCatalog()}

YOUR JOB:
1. Understand the user's research topic or business problem.
2. Recommend the most relevant dataset(s) — mention the dataset ID in brackets like [bog_inflation].
3. Suggest 2-3 appropriate analysis methods.
4. Suggest 2-3 sample research objectives.
5. Always end by directing them to: https://sgdatalytics.org/marketplace.html

RULES:
- Only recommend datasets from the catalog above.
- Be warm, professional, and concise (under 400 words).
- If the user's need is unclear, ask ONE focused follow-up question.
- Always mention dataset IDs in square brackets when recommending.`;

// ── Extract datasets mentioned in the reply ───────────────────────────────────
function extractDatasets(text) {
  return DATASETS.filter(d => text.includes(`[${d.id}]`)).map(d => ({
    id: d.id, name: d.name, sector: d.sector,
    description: `Best for: ${d.bestFor.slice(0,3).join(", ")}`,
    suggested_methods: d.methods,
    marketplace_link: "https://sgdatalytics.org/marketplace.html",
  }));
}

// ── Main handler ──────────────────────────────────────────────────────────────
exports.handler = async (event) => {
  // CORS headers
  const headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Content-Type": "application/json",
  };

  // Handle preflight
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers, body: "" };
  }

  if (event.httpMethod !== "POST") {
    return { statusCode: 405, headers, body: JSON.stringify({ error: "Method not allowed" }) };
  }

  let body;
  try {
    body = JSON.parse(event.body);
  } catch {
    return { statusCode: 400, headers, body: JSON.stringify({ error: "Invalid JSON" }) };
  }

  const { messages = [], api_key = "" } = body;
  const apiKey = api_key || process.env.GOOGLE_API_KEY || "";

  if (!apiKey) {
    return {
      statusCode: 401,
      headers,
      body: JSON.stringify({ error: "No Google API key provided. Set GOOGLE_API_KEY in Netlify environment variables." }),
    };
  }

  // Build conversation contents for Google API
  const contents = messages.map(m => ({
    role: m.role === "assistant" ? "model" : "user",
    parts: [{ text: m.content }],
  }));

  const googlePayload = {
    system_instruction: { parts: [{ text: SYSTEM_PROMPT }] },
    contents,
    generationConfig: { temperature: 0.4, maxOutputTokens: 1024 },
  };

  const model = process.env.GOOGLE_MODEL || "gemma-4-31b-it";
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(googlePayload),
    });

    if (!response.ok) {
      const errText = await response.text();
      return {
        statusCode: response.status,
        headers,
        body: JSON.stringify({ error: `Google API error: ${errText.slice(0, 300)}` }),
      };
    }

    const data = await response.json();
    const reply = data?.candidates?.[0]?.content?.parts?.[0]?.text || "Sorry, I could not generate a response.";
    const datasets_found = extractDatasets(reply);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        reply,
        datasets_found,
        tool_calls: [{ tool: "search_datasets", input: { query: messages.at(-1)?.content || "" } }],
      }),
    };
  } catch (err) {
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: `Function error: ${err.message}` }),
    };
  }
};
