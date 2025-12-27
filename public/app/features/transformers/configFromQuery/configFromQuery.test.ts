import { toDataFrame, FieldType, ReducerID, DataFrame } from '@grafana/data';

import { FieldConfigHandlerKey } from '../fieldToConfigMapping/fieldToConfigMapping';

import { extractConfigFromQuery, ConfigFromQueryTransformOptions } from './configFromQuery';

describe('config from data', () => {
  const config = toDataFrame({
    fields: [
      { name: 'Time', type: FieldType.time, values: [1, 2] },
      { name: 'Max', type: FieldType.number, values: [1, 10, 50] },
      { name: 'Min', type: FieldType.number, values: [1, 10, 5] },
      { name: 'Names', type: FieldType.string, values: ['first-name', 'middle', 'last-name'] },
    ],
    refId: 'A',
  });

  const seriesA = toDataFrame({
    fields: [
      { name: 'Time', type: FieldType.time, values: [1, 2, 3] },
      {
        name: 'Value',
        type: FieldType.number,
        values: [2, 3, 4],
        config: { displayName: 'SeriesA' },
      },
    ],
  });

  it('Select and apply with two frames and default mappings and reducer', () => {
    const options: ConfigFromQueryTransformOptions = {
      configRefId: 'A',
      mappings: [],
    };

    const results = extractConfigFromQuery(options, [config, seriesA]);
    expect(results.length).toBe(1);
    expect(results[0].fields[1].config.max).toBe(50);
    expect(results[0].fields[1].config.min).toBe(5);
  });

  it('Can apply to config frame if there is only one frame', () => {
    const options: ConfigFromQueryTransformOptions = {
      configRefId: 'A',
      mappings: [],
    };

    const results = extractConfigFromQuery(options, [config]);
    expect(results.length).toBe(1);
    expect(results[0].fields[1].name).toBe('Max');
    expect(results[0].fields[1].config.max).toBe(50);
  });

  it('With ignore mappings', () => {
    const options: ConfigFromQueryTransformOptions = {
      configRefId: 'A',
      mappings: [{ fieldName: 'Min', handlerKey: FieldConfigHandlerKey.Ignore }],
    };

    const results = extractConfigFromQuery(options, [config, seriesA]);
    expect(results.length).toBe(1);
    expect(results[0].fields[1].config.min).toEqual(undefined);
    expect(results[0].fields[1].config.max).toEqual(50);
  });

  it('With custom mappings', () => {
    const options: ConfigFromQueryTransformOptions = {
      configRefId: 'A',
      mappings: [{ fieldName: 'Min', handlerKey: 'decimals' }],
    };

    const results = extractConfigFromQuery(options, [config, seriesA]);
    expect(results.length).toBe(1);
    expect(results[0].fields[1].config.decimals).toBe(5);
  });

  it('With custom reducer', () => {
    const options: ConfigFromQueryTransformOptions = {
      configRefId: 'A',
      mappings: [{ fieldName: 'Max', handlerKey: 'max', reducerId: ReducerID.min }],
    };

    const results = extractConfigFromQuery(options, [config, seriesA]);
    expect(results.length).toBe(1);
    expect(results[0].fields[1].config.max).toBe(1);
  });

  it('With threshold', () => {
    const options: ConfigFromQueryTransformOptions = {
      configRefId: 'A',
      mappings: [{ fieldName: 'Max', handlerKey: 'threshold1', handlerArguments: { threshold: { color: 'orange' } } }],
    };

    const results = extractConfigFromQuery(options, [config, seriesA]);
    expect(results.length).toBe(1);
    const thresholdConfig = results[0].fields[1].config.thresholds?.steps[0];
    expect(thresholdConfig).toBeDefined();
    expect(thresholdConfig?.color).toBe('orange');
    expect(thresholdConfig?.value).toBe(50);
  });

  it('With custom matcher and displayName mapping', () => {
    const options: ConfigFromQueryTransformOptions = {
      configRefId: 'A',
      mappings: [{ fieldName: 'Names', handlerKey: 'displayName', reducerId: ReducerID.first }],
      applyTo: { id: 'byName', options: 'Value' },
    };

    const results = extractConfigFromQuery(options, [config, seriesA]);
    expect(results.length).toBe(1);
    expect(results[0].fields[1].config.displayName).toBe('first-name');
  });
});

describe('value mapping from data', () => {
  const config = toDataFrame({
    fields: [
      { name: 'value', type: FieldType.number, values: [1, 2, 3] },
      { name: 'threshold', type: FieldType.number, values: [4] },
      { name: 'text', type: FieldType.string, values: ['one', 'two', 'three'] },
      { name: 'color', type: FieldType.string, values: ['red', 'blue', 'green'] },
    ],
    refId: 'config',
  });

  const seriesA = toDataFrame({
    fields: [
      { name: 'Time', type: FieldType.time, values: [1, 2, 3] },
      {
        name: 'Value',
        type: FieldType.number,
        values: [1, 2, 3],
        config: {},
      },
    ],
  });

  it('Should take all field values and map to value mappings', () => {
    const options: ConfigFromQueryTransformOptions = {
      configRefId: 'config',
      mappings: [
        { fieldName: 'value', handlerKey: 'mappings.value' },
        { fieldName: 'color', handlerKey: 'mappings.color' },
        { fieldName: 'text', handlerKey: 'mappings.text' },
      ],
    };

    const results = extractConfigFromQuery(options, [config, seriesA]);
    expect(results[0].fields[1].config.mappings).toMatchInlineSnapshot(`
      [
        {
          "options": {
            "1": {
              "color": "red",
              "index": 0,
              "text": "one",
            },
            "2": {
              "color": "blue",
              "index": 1,
              "text": "two",
            },
            "3": {
              "color": "green",
              "index": 2,
              "text": "three",
            },
          },
          "type": "value",
        },
      ]
    `);
  });
});

describe('multiple threshold transforms (mergeConfig behavior)', () => {
  const valueConfig = {
    thresholds: { mode: 'absolute', steps: [{ value: -Infinity, color: 'green' }] },
  };

  const series = toDataFrame({
    fields: [
      { name: 'Time', type: FieldType.time, values: [1, 2, 3] },
      { name: 'Value', type: FieldType.number, values: [10, 60, 90], config: valueConfig },
    ],
  });

  const applyThreshold = (data: DataFrame[], value: number, color: string, refId = 'Config') => {
    const config = toDataFrame({
      fields: [{ name: 'Threshold', type: FieldType.number, values: [value] }],
      refId,
    });

    const options: ConfigFromQueryTransformOptions = {
      configRefId: refId,
      mappings: [
        {
          fieldName: 'Threshold',
          handlerKey: 'threshold1',
          handlerArguments: { threshold: { color } },
        },
      ],
    };

    return extractConfigFromQuery(options, [config, ...data]);
  };

  const getSteps = (data: DataFrame[]) => data[0].fields[1].config.thresholds?.steps || [];
  const getValues = (data: DataFrame[]) => getSteps(data).map((s) => s.value);
  const getColors = (data: DataFrame[]) => getSteps(data).map((s) => s.color);

  it('merges thresholds from multiple transforms', () => {
    let result = applyThreshold([series], 50, 'yellow', 'Config1');
    result = applyThreshold(result, 80, 'red', 'Config2');

    expect(getValues(result)).toEqual([-Infinity, 50, 80]);
    expect(getColors(result)).toEqual(['green', 'yellow', 'red']);
  });

  it('deduplicates thresholds with same value (keeps last)', () => {
    let result = applyThreshold([series], 50, 'yellow', 'Config1');
    result = applyThreshold(result, 50, 'orange', 'Config2');

    expect(getValues(result)).toEqual([-Infinity, 50]);
    expect(getColors(result)).toEqual(['green', 'orange']);
  });

  it('sorts thresholds by value', () => {
    let result = applyThreshold([series], 80, 'red', 'Config1');
    result = applyThreshold(result, 50, 'yellow', 'Config2');

    expect(getValues(result)).toEqual([-Infinity, 50, 80]);
    expect(getColors(result)).toEqual(['green', 'yellow', 'red']);
  });

  it('merges three thresholds', () => {
    let result = applyThreshold([series], 30, 'blue', 'Config1');
    result = applyThreshold(result, 60, 'yellow', 'Config2');
    result = applyThreshold(result, 90, 'red', 'Config3');

    expect(getValues(result)).toEqual([-Infinity, 30, 60, 90]);
    expect(getColors(result)).toEqual(['green', 'blue', 'yellow', 'red']);
  });
});
