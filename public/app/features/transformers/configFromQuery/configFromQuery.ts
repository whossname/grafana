import { map } from 'rxjs/operators';

import {
  DataFrame,
  DataTransformerID,
  DataTransformerInfo,
  FieldConfig,
  FieldMatcherID,
  getFieldDisplayName,
  getFieldMatcher,
  MatcherConfig,
  reduceField,
} from '@grafana/data';
import { t } from '@grafana/i18n';

import {
  evaluateFieldMappings,
  FieldToConfigMapping,
  getFieldConfigFromFrame,
} from '../fieldToConfigMapping/fieldToConfigMapping';

export interface ConfigFromQueryTransformOptions {
  configRefId?: string;
  mappings: FieldToConfigMapping[];
  applyTo?: MatcherConfig;
}

function mergeConfig<TOptions = any>(
  target: FieldConfig<TOptions>,
  source: FieldConfig<TOptions>
): FieldConfig<TOptions> {
  const result = { ...target, ...source } as FieldConfig<TOptions>;
  const targetSteps = target.thresholds?.steps;
  const sourceSteps = source.thresholds?.steps;

  // merge steps (multiple transforms)
  if (Array.isArray(targetSteps) && Array.isArray(sourceSteps)) {
    const merged = [...targetSteps, ...sourceSteps].sort((a, b) => (a as any).value - (b as any).value);
    const deduped = merged.filter(
      (step, index) => index === merged.length - 1 || step.value !== merged[index + 1].value
    );
    result.thresholds!.steps = deduped;
  }

  return result;
}

export function extractConfigFromQuery(options: ConfigFromQueryTransformOptions, data: DataFrame[]) {
  let configFrame: DataFrame | null = null;

  for (const frame of data) {
    if (frame.refId === options.configRefId) {
      configFrame = frame;
      break;
    }
  }

  if (!configFrame) {
    return data;
  }

  const reducedConfigFrame: DataFrame = {
    fields: [],
    length: 1,
  };

  const mappingResult = evaluateFieldMappings(configFrame, options.mappings ?? [], false);

  // reduce config frame
  for (const field of configFrame.fields) {
    const newField = { ...field };
    const fieldName = getFieldDisplayName(field, configFrame);
    const fieldMapping = mappingResult.index[fieldName];
    const result = reduceField({ field, reducers: [fieldMapping.reducerId] });
    newField.values = [result[fieldMapping.reducerId]];
    reducedConfigFrame.fields.push(newField);
  }

  const output: DataFrame[] = [];
  const matcher = getFieldMatcher(options.applyTo || { id: FieldMatcherID.numeric });

  for (const frame of data) {
    // Skip config frame in output
    if (frame === configFrame && data.length > 1) {
      continue;
    }

    const outputFrame: DataFrame = {
      fields: [],
      length: frame.length,
      refId: frame.refId,
    };

    for (const field of frame.fields) {
      if (matcher(field, frame, data)) {
        const dataConfig = getFieldConfigFromFrame(reducedConfigFrame, 0, mappingResult);
        const config = mergeConfig(field.config, dataConfig);
        outputFrame.fields.push({ ...field, config });
      } else {
        outputFrame.fields.push(field);
      }
    }

    output.push(outputFrame);
  }
  return output;
}

export const getConfigFromDataTransformer: () => DataTransformerInfo<ConfigFromQueryTransformOptions> = () => ({
  id: DataTransformerID.configFromData,
  name: t('transformers.get-config-from-data-transformer.name.config-from-query-results', 'Config from query results'),
  description: t(
    'transformers.get-config-from-data-transformer.description.set-unit-min-max-and-more',
    'Set unit, min, max and more.'
  ),
  defaultOptions: {
    configRefId: 'config',
    mappings: [],
  },

  /**
   * Return a modified copy of the series. If the transform is not or should not
   * be applied, just return the input series
   */
  operator: (options) => (source) => source.pipe(map((data) => extractConfigFromQuery(options, data))),
});
