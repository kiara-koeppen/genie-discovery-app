import {
  Typography, Box, Accordion, AccordionSummary, AccordionDetails, Alert,
  TextField, Chip, Paper,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";

interface Props {
  data: Record<string, any>;
  onChange: (section: string, value: any) => void;
  readOnly?: boolean;
  session3Data?: Record<string, any>;
  session4Data?: Record<string, any>;
  engagementId?: string;
}

export default function Session5Form({
  data, onChange, readOnly, session3Data, session4Data,
}: Props) {
  const dataPlan = session4Data?.data_plan || [];
  const includedItems = dataPlan.filter((d: any) => d.include_in_space === "Yes");
  const sqlExprs = session3Data?.sql_expressions || [];
  const textInstrs = session3Data?.text_instructions || [];

  return (
    <Box>
      <Alert severity="info" sx={{ mb: 2 }}>
        <strong>Configure Genie Space:</strong> Connect to an existing Genie Space and push
        the configuration collected in Sessions 1-3. Review the configuration preview below
        before applying.
      </Alert>

      {/* Genie Space Selection */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Genie Space</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Enter the ID or URL of the existing Genie Space you want to configure.
            This space should already be created and accessible to you.
          </Typography>
          <TextField
            fullWidth
            label="Genie Space ID"
            placeholder="Paste Genie Space ID or URL"
            value={data.genie_space_id || ""}
            onChange={(e) => onChange("genie_space_id", e.target.value)}
            disabled={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      {/* Configuration Preview */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Typography variant="h6">Configuration Preview</Typography>
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            This is what will be pushed to the Genie Space based on your Sessions 1-3 data
            and the approved data plan.
          </Typography>

          {/* Tables */}
          <Typography variant="subtitle2" sx={{ mt: 1, mb: 0.5 }}>
            Tables & Views ({includedItems.length})
          </Typography>
          {includedItems.length > 0 ? (
            <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5, mb: 2 }}>
              {includedItems.map((item: any, i: number) => (
                <Chip
                  key={i}
                  label={item.table_or_view}
                  size="small"
                  color={item.type === "Metric View" ? "primary" : "default"}
                  sx={{ fontFamily: "monospace", fontSize: 12 }}
                />
              ))}
            </Box>
          ) : (
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              No tables/views marked for inclusion in the data plan.
            </Typography>
          )}

          {/* SQL Expressions */}
          <Typography variant="subtitle2" sx={{ mt: 1, mb: 0.5 }}>
            SQL Expressions ({sqlExprs.length})
          </Typography>
          {sqlExprs.length > 0 ? (
            <Paper variant="outlined" sx={{ p: 1.5, mb: 2 }}>
              {sqlExprs.map((e: any, i: number) => (
                <Box key={i} sx={{ mb: i < sqlExprs.length - 1 ? 1 : 0 }}>
                  <Typography variant="body2">
                    <strong>{e.metric_name}</strong> on <code>{e.uc_table}</code>
                  </Typography>
                  {e.sql_code && (
                    <Typography variant="body2" sx={{ fontFamily: "monospace", fontSize: 12, color: "text.secondary" }}>
                      {e.sql_code}
                    </Typography>
                  )}
                </Box>
              ))}
            </Paper>
          ) : (
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              No SQL expressions defined.
            </Typography>
          )}

          {/* Text Instructions */}
          <Typography variant="subtitle2" sx={{ mt: 1, mb: 0.5 }}>
            Text Instructions ({textInstrs.length})
          </Typography>
          {textInstrs.length > 0 ? (
            <Paper variant="outlined" sx={{ p: 1.5, mb: 2 }}>
              {textInstrs.map((t: any, i: number) => (
                <Box key={i} sx={{ mb: i < textInstrs.length - 1 ? 1 : 0 }}>
                  <Typography variant="body2">
                    <strong>{t.title}:</strong> {t.instruction}
                  </Typography>
                </Box>
              ))}
            </Paper>
          ) : (
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              No text instructions defined.
            </Typography>
          )}

          <Alert severity="warning" sx={{ mt: 2 }}>
            The "Push to Genie Space" functionality is coming soon. For now, use this preview
            to manually configure your Genie Space with the information above.
          </Alert>
        </AccordionDetails>
      </Accordion>
    </Box>
  );
}
