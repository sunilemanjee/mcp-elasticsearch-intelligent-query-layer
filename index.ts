#!/usr/bin/env node

/*
 * Copyright Elasticsearch B.V. and contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { Client, estypes, ClientOptions } from "@elastic/elasticsearch";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import fs from "fs";
import axios from "axios";



// Configuration schema with auth options
const ConfigSchema = z
  .object({
    url: z
      .string()
      .trim()
      .min(1, "Elasticsearch URL cannot be empty")
      .url("Invalid Elasticsearch URL format")
      .describe("Elasticsearch server URL"),

    apiKey: z
      .string()
      .optional()
      .describe("API key for Elasticsearch authentication"),

    username: z
      .string()
      .optional()
      .describe("Username for Elasticsearch authentication"),

    password: z
      .string()
      .optional()
      .describe("Password for Elasticsearch authentication"),

    caCert: z
      .string()
      .optional()
      .describe("Path to custom CA certificate for Elasticsearch"),
    
    googleMapsApiKey: z
      .string()
      .optional()
      .describe("Google Maps API key for geocoding functionality"),

    propertiesSearchTemplate: z
      .string()
      .optional()
      .default("properties-search-template")
      .describe("ID of the search template for properties"),

      inferenceId: z
      .string()
      .optional()
      .default(".elser-2-elasticsearch")
      .describe("ID of the Elasticsearch ELSER inference endpoint to check"),

  })
  .refine(
    (data) => {
      // If username is provided, password must be provided
      if (data.username) {
        return !!data.password;
      }

      // If password is provided, username must be provided
      if (data.password) {
        return !!data.username;
      }

      // If apiKey is provided, it's valid
      if (data.apiKey) {
        return true;
      }

      // No auth is also valid (for local development)
      return true;
    },
    {
      message:
        "Either ES_API_KEY or both ES_USERNAME and ES_PASSWORD must be provided, or no auth for local development",
      path: ["username", "password"],
    }
  );

type ElasticsearchConfig = z.infer<typeof ConfigSchema>;

// Create a proper JSON logger with TypeScript type annotations
const logger = {
  log: (message: string | object | any[]): void => {
    // Ensure the message is a string
    const strMessage = typeof message === 'string' 
      ? message 
      : JSON.stringify(message);
    
    // Log in a format that won't break JSON parsing
    process.stderr.write(JSON.stringify({ level: 'info', message: strMessage }) + '\n');
  },
  
  error: (message: string | object | any[]): void => {
    // Ensure the message is a string
    const strMessage = typeof message === 'string' 
      ? message 
      : JSON.stringify(message);
    
    // Log in a format that won't break JSON parsing
    process.stderr.write(JSON.stringify({ level: 'error', message: strMessage }) + '\n');
  }
};


// Function to check if the Elasticsearch inference endpoint is ready
async function waitForInferenceEndpoint(
  client: Client, 
  inferenceId: string, 
  timeoutSeconds: number = 60
): Promise<boolean> {
  logger.log(`Checking inference endpoint ${inferenceId} with ${timeoutSeconds}s timeout...`);
  
  try {
    // Use the Elasticsearch client to make a request to the inference endpoint
    // with a simple input and long timeout
    const response = await client.transport.request({
      method: 'POST',
      path: `/_inference/${inferenceId}`,
      body: {
        task_type: "sparse_embedding",
        input: "wake up"
      },
      querystring: {
        timeout: `${timeoutSeconds}s`
      }
    });
    
    logger.log(`Inference endpoint is ready: ${inferenceId}`);
    return true;
  } catch (error) {
    logger.error(
      `Failed to connect to inference endpoint: ${
        error instanceof Error ? error.message : String(error)
      }`
    );
    return false;
  }
}

export async function createElasticsearchMcpServer(
  config: ElasticsearchConfig
) {
  const validatedConfig = ConfigSchema.parse(config);
  const { url, apiKey, username, password, caCert, googleMapsApiKey, propertiesSearchTemplate, inferenceId } = validatedConfig;

  const clientOptions: ClientOptions = {
    node: url,
  };

  // Set up authentication
  if (apiKey) {
    clientOptions.auth = { apiKey };
  } else if (username && password) {
    clientOptions.auth = { username, password };
  }

  // Set up SSL/TLS certificate if provided
  if (caCert) {
    try {
      const ca = fs.readFileSync(caCert);
      clientOptions.tls = { ca };
    } catch (error) {
      logger.error(
        `Failed to read certificate file: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }
  }

  const esClient = new Client(clientOptions);

  // Check the inference endpoint if an ID is provided
  if (inferenceId) {
    logger.log(`Checking inference endpoint: ${inferenceId}`);
    try {
      const isReady = await waitForInferenceEndpoint(esClient, inferenceId, 60);
      if (!isReady) {
        logger.error(`Inference endpoint ${inferenceId} is not available after timeout`);
        // Decide whether to continue or throw an error based on your requirements
        // If it's crucial for the application, you might want to throw an error here
      }
    } catch (error) {
      logger.error(`Error checking inference endpoint: ${
        error instanceof Error ? error.message : String(error)
      }`);
    }
  }

  const server = new McpServer({
    name: "elasticsearch-mcp-server",
    version: "0.1.1",
  });

// Tool: Get properties search template parameters
server.tool(
  "get_properties_template_params",
  "Get the required parameters for the properties search template",
  {},
  async () => {
    try {
      const template_id = propertiesSearchTemplate;
      
      // Use a direct API call to get the template with proper typing
      const response = await esClient.transport.request({
        method: 'GET',
        path: `/_scripts/${template_id}`
      }) as any; // Use 'any' type to avoid TypeScript errors
      
      // Safely extract the script source
      let source = "Template source not available";
      
      // Check if response has the expected structure
      if (response && typeof response === 'object' && 'script' in response) {
        const scriptObj = response.script;
        if (scriptObj && typeof scriptObj === 'object' && 'source' in scriptObj) {
          source = scriptObj.source as string;
        }
      } else {
        logger.log(`Template ${template_id} structure: ${JSON.stringify(response)}`);
      }
      
      // Analyze the template to identify required parameters
      const paramMatches = source.match(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g) || [];
      const parameters = [...new Set(paramMatches.map(m => 
        m.replace(/\{\{\s*/, '').replace(/\s*\}\}/, '')
      ))];

      logger.log(`Found parameters for template ${template_id}: ${parameters.join(', ')}`);

      return {
        content: [
          {
            type: "text" as const,
            text: `Required parameters for properties search template:`,
          },
          {
            type: "text" as const,
            text: parameters.join(', '),
          },
          {
            type: "text" as const,
            text: `Parameter descriptions:`,
          },
          {
            type: "text" as const,
            text: `- query: Main search query (mandatory)
- latitude: Geographic latitude coordinate
- longitude: Geographic longitude coordinate
- bathrooms: Number of bathrooms
- tax: Real estate tax amount
- maintenance: Maintenance fee amount
- square_footage: Property square footage
- home_price: Max home price. Not a range, just a number
- features: Home features such as AC, pool, updated kitches, etc. the features should be enclosed in *. For example features such as pool and updated kitchen should be formated as *pool*updated kitchen*`,
          },
        ],
        data: {
          parameters: parameters
        }
      };
    } catch (error) {
      logger.error(
        `Failed to get template parameters: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
      return {
        content: [
          {
            type: "text" as const,
            text: `Error: ${
              error instanceof Error ? error.message : String(error)
            }`,
          },
        ],
      };
    }
  }
);


// Tool 3: Geocode location
server.tool(
  "geocode_location",
  "Geocode a location string into a geo_point",
  {
    location: z
      .string()
      .trim()
      .min(1, "Location string is required")
      .describe("Location as a human-readable string (e.g., 'Surfside Beach, Texas')"),
  },
  async ({ location }) => {
    try {
      // Check if API key is available
      if (!googleMapsApiKey) {
        logger.error("No Google Maps API key provided");
        return {
          content: [
            {
              type: "text" as const,
              text: "Error: Google Maps API key not configured",
            },
          ],
        };
      }

      const baseUrl = "https://maps.googleapis.com/maps/api/geocode/json";
      const query = `${baseUrl}?address=${encodeURIComponent(location)}&region=us&key=${googleMapsApiKey}`;

      logger.log(`Attempting to geocode: "${location}"`);
      const response = await axios.get(query);
      
      // Log the status of the API response
      logger.log(`Geocoding status: ${response.data?.status}`);
      
      // Check the API response status before proceeding
      if (response.data?.status !== "OK") {
        logger.error(`Google API error: ${response.data?.status} - ${response.data?.error_message || 'No detailed error message'}`);
        return {
          content: [
            {
              type: "text" as const,
              text: `Geocoding failed: ${response.data?.status || 'Unknown error'} for location "${location}"`,
            },
          ],
        };
      }
      
      let result = response.data?.results?.[0];

      // Try fallback variations if needed
      if (!result) {
        logger.log("No results found, trying variations...");
        
        // Try with "TX" replaced by "Texas"
        if (location.includes("TX")) {
          const fallbackLocation = location.replace("TX", "Texas");
          logger.log(`Trying fallback: "${fallbackLocation}"`);
          const fallbackQuery = `${baseUrl}?address=${encodeURIComponent(fallbackLocation)}&region=us&key=${googleMapsApiKey}`;
          const fallbackResponse = await axios.get(fallbackQuery);
          result = fallbackResponse.data?.results?.[0];
        }
        
        // Try without any state abbreviation
        if (!result) {
          const stateIndex = location.lastIndexOf(",");
          if (stateIndex > 0) {
            const fallbackLocation = location.substring(0, stateIndex).trim();
            logger.log(`Trying without state: "${fallbackLocation}"`);
            const fallbackQuery = `${baseUrl}?address=${encodeURIComponent(fallbackLocation)}&region=us&key=${googleMapsApiKey}`;
            const fallbackResponse = await axios.get(fallbackQuery);
            result = fallbackResponse.data?.results?.[0];
          }
        }
      }

      if (!result || !result.geometry?.location) {
        logger.error("No geocoding results found after all attempts");
        return {
          content: [
            {
              type: "text" as const,
              text: `Could not geocode location: "${location}"`,
            },
          ],
        };
      }

      // Use the expected parameter names (latitude/longitude) instead of lat/lon
      const geoPoint = {
        latitude: result.geometry.location.lat,
        longitude: result.geometry.location.lng
      };

      logger.log(`Successfully geocoded to: ${JSON.stringify(geoPoint)}`);
      return {
        content: [
          {
            type: "text" as const,
            text: `Geocoded "${location}" to: ${JSON.stringify(geoPoint)}`,
          },
        ],
        data: geoPoint,
      };
    } catch (error) {
      logger.error(`Geocoding error: ${error instanceof Error ? error.message : String(error)}`);
      return {
        content: [
          {
            type: "text" as const,
            text: `Error: ${
              error instanceof Error ? error.message : String(error)
            }`,
          },
        ],
      };
    }
  }
);

// Tool: Search Template with parameter normalization
server.tool(
  "search_template",
  "Execute a pre-defined Elasticsearch search template with provided parameters.",
  {
    index: z
      .string()
      .trim()
      .min(1, "Index name is required")
      .describe("Name of the Elasticsearch index to search"),

    template_id: z
      .string()
      .trim()
      .min(1, "Template ID is required")
      .describe("ID of the stored search template to use"),

    params: z.record(z.any()).describe("Parameters to pass to the template"),
    
    original_query: z
      .string()
      .describe("The complete original query from the user")
  },
  async ({ index, template_id, params, original_query }) => {
    try {
      // Always use properties-search-template for properties index
      const effectiveTemplateId = "properties-search-template";
      const effectiveIndex = "properties";
      
      // Normalize parameters for properties search
      let normalizedParams = { ...params };
      
      // Use the full original query as the query parameter
      normalizedParams.query = original_query;
      
      // Convert lat/lon to latitude/longitude if needed
      if (normalizedParams.lat !== undefined && normalizedParams.lon !== undefined) {
        logger.log("Converting lat/lon to latitude/longitude");
        
        // Add latitude/longitude parameters while preserving original lat/lon
        normalizedParams.latitude = normalizedParams.lat;
        normalizedParams.longitude = normalizedParams.lon;
      }
      
      // Convert distance format if needed (20mi -> 20miles)
      if (normalizedParams.distance && typeof normalizedParams.distance === 'string') {
        if (normalizedParams.distance.endsWith('mi') && !normalizedParams.distance.endsWith('miles')) {
          normalizedParams.distance = normalizedParams.distance.replace('mi', 'miles');
          logger.log(`Normalized distance: ${normalizedParams.distance}`);
        }
      }
      // Convert distance format if needed
      if (normalizedParams.distance) {
        // If it's just a number, add "miles" as the default unit
        if (typeof normalizedParams.distance === 'number') {
          normalizedParams.distance = `${normalizedParams.distance}miles`;
          logger.log(`Added default unit to distance: ${normalizedParams.distance}`);
        } 
        // If it's a string, ensure proper units
        else if (typeof normalizedParams.distance === 'string') {
          // If it has no units, add "miles"
          if (/^\d+$/.test(normalizedParams.distance)) {
            normalizedParams.distance = `${normalizedParams.distance}miles`;
            logger.log(`Added default unit to distance: ${normalizedParams.distance}`);
          }
          // If it has "mi" but not "miles", convert it
          else if (normalizedParams.distance.endsWith('mi') && !normalizedParams.distance.endsWith('miles')) {
            normalizedParams.distance = normalizedParams.distance.replace('mi', 'miles');
            logger.log(`Normalized distance: ${normalizedParams.distance}`);
          }
          // If no unit or unrecognized unit, add "miles"
          else if (!normalizedParams.distance.includes('miles') && !normalizedParams.distance.includes('km')) {
            normalizedParams.distance = `${normalizedParams.distance}miles`;
            logger.log(`Added default unit to distance: ${normalizedParams.distance}`);
          }
        }
      }


      // Fix home_price format if needed
      if (normalizedParams.home_price && typeof normalizedParams.home_price === 'string') {
        // If it's in a range format like "0-500000", extract just the upper limit
        if (normalizedParams.home_price.includes('-')) {
          const parts = normalizedParams.home_price.split('-');
          const upperLimit = parts[1];
          if (upperLimit && !isNaN(Number(upperLimit))) {
            normalizedParams.home_price = Number(upperLimit);
            logger.log(`Extracted upper limit from home_price range: ${normalizedParams.home_price}`);
          }
        }
      }

      logger.log(`Using template ID: ${effectiveTemplateId} for index: ${effectiveIndex}`);
      logger.log(`Original user query: ${original_query}`);
      logger.log(`Normalized parameters: ${JSON.stringify(normalizedParams)}`);

      const result = await esClient.searchTemplate({
        index: effectiveIndex,
        id: effectiveTemplateId,
        params: normalizedParams,
      });

      const from = normalizedParams.from || 0;
      const contentFragments = result.hits.hits.map((hit) => {
        const highlightedFields = hit.highlight || {};
        const sourceData = hit._source || {};

        let content = "";

        for (const [field, highlights] of Object.entries(highlightedFields)) {
          if (highlights && highlights.length > 0) {
            content += `${field} (highlighted): ${highlights.join(" ... ")}\n`;
          }
        }

        for (const [field, value] of Object.entries(sourceData)) {
          if (!(field in highlightedFields)) {
            content += `${field}: ${JSON.stringify(value)}\n`;
          }
        }

        return {
          type: "text" as const,
          text: content.trim(),
        };
      });

      const metadataFragment = {
        type: "text" as const,
        text: `Total results: ${
          typeof result.hits.total === "number"
            ? result.hits.total
            : result.hits.total?.value || 0
        }, showing ${result.hits.hits.length} from position ${from}. Maximum of 5 results are shown with ALL available property details included. No additional API calls are needed to get more details about these properties.`,
      };

      return {
        content: [metadataFragment, ...contentFragments],
      };
    } catch (error) {
      logger.error(
        `Search template failed: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
      return {
        content: [
          {
            type: "text" as const,
            text: `Error: ${
              error instanceof Error ? error.message : String(error)
            }`,
          },
        ],
      };
    }
  }
);
  
  return server;
}

const config: ElasticsearchConfig = {
  url: process.env.ES_URL || "",
  apiKey: process.env.ES_API_KEY || "",
  username: process.env.ES_USERNAME || "",
  password: process.env.ES_PASSWORD || "",
  caCert: process.env.ES_CA_CERT || "",
  googleMapsApiKey: process.env.GOOGLE_MAPS_API_KEY || "",
  propertiesSearchTemplate: process.env.PROPERTIES_SEARCH_TEMPLATE || "",
  inferenceId: process.env.ELSER_INFERENCE_ID || "",

};

async function main() {
  const transport = new StdioServerTransport();
  const server = await createElasticsearchMcpServer(config);

  await server.connect(transport);

  process.on("SIGINT", async () => {
    await server.close();
    process.exit(0);
  });
}

// Properly format the error message in the catch block
main().catch((error) => {
  // Combine the prefix and error message into a single string
  const errorMessage = `Server error: ${
    error instanceof Error ? error.message : String(error)
  }`;
  
  logger.error(errorMessage);
  process.exit(1);
});