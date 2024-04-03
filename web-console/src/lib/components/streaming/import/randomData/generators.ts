// Contains a lists of random generators for every SQL type, each random
// generator may have custom generation and validation methods.

import { BigNumberInput } from '$lib/components/input/BigNumberInput'
import { NumberInput } from '$lib/components/input/NumberInput'
import { clampBigNumber } from '$lib/functions/common/bigNumber'
import {
  randomExponentialBigNumber,
  randomIntBigNumber,
  randomNormalBigNumber
} from '$lib/functions/common/d3-random-bignumber'
import { nonNull } from '$lib/functions/common/function'
import { tuple } from '$lib/functions/common/tuple'
import { bignumber, maxBigNumber, minBigNumber } from '$lib/functions/common/valibot'
import { dateTimeRange, findBaseType, numericRange } from '$lib/functions/sqlValue'
import { ColumnType, Field } from '$lib/services/manager'
import { BigNumber } from 'bignumber.js/bignumber.js'
import * as d3 from 'd3-random'
import dayjs, { Dayjs } from 'dayjs'
import invariant from 'tiny-invariant'
import { match, P } from 'ts-pattern'
import * as va from 'valibot'

import { faker } from '@faker-js/faker'
import { SwitchProps, TextField, TextFieldProps } from '@mui/material'
import {
  DatePicker,
  DatePickerProps,
  DateRangePicker,
  DateRangePickerProps,
  DateTimePicker,
  DateTimePickerProps,
  MultiInputDateTimeRangeField,
  MultiInputDateTimeRangeFieldProps,
  TimePicker,
  TimePickerProps
} from '@mui/x-date-pickers-pro'

import { FieldNames } from './'
import { BooleanSwitch } from './BooleanSwitch'

import type { SQLValueJS } from '$lib/functions/sqlValue'

type AddFieldName<T> = { name: FieldNames } & Partial<T>

// A list of categories for grouping the generators in the selector.
export enum Categories {
  DEFAULT = '',
  DISTRIBUTION = 'Distribution',
  RAND_STRINGS = 'Lorem Ipsum',
  PERSON = 'Person',
  PHONE_NUMBER = 'Phone Number',
  FINANCE = 'Finance',
  TRAVEL = 'Travel',
  COMMERCE = 'Commerce',
  COMPUTER = 'IT',
  LOCATION = 'Location',
  PHYSICAL = 'Physical'
}

// Describes an RNG generator method.
export interface IRngGenMethod {
  // Name of the method, the title must be unique in the whole list of options
  // for a given type.
  title: string
  // Which category this method belongs to (for grouping in the selector).
  category: Categories
  // The random generator function, receives the type, plus additional
  // properties (coming from the defined `form_fields` see below) to generate a
  // random value.
  generator: (config: ColumnType, settings?: Partial<Record<FieldNames, any>>) => SQLValueJS
  // Additional form fields that are displayed when this generation method is
  // selected.
  //
  // Note: The `name` field from the `props` can be used to index settings in
  // the to the `generator` function (to read the value of the form field).
  form_fields?: (config: Field) => {
    // How much grid space this field should take up
    sm: number
    // The compnent to render for the form
    component: React.FunctionComponent<any>
    // The props to pass to the component
    props: AddFieldName<
      | TextFieldProps
      | SwitchProps
      | TimePickerProps<Dayjs>
      | DatePickerProps<Dayjs>
      | DateRangePickerProps<Dayjs>
      | DateTimePickerProps<Dayjs>
      | MultiInputDateTimeRangeFieldProps<Dayjs>
    >
  }[]
  // A validation schema for the form fields to display errors in case an
  // invalid value is entered in a form.
  validationSchema?: (field: ColumnType) => va.ObjectSchema<any>
}

const getDefaultRngMethodName = (sqlType: ColumnType): string => {
  return match(sqlType)
    .with({ type: undefined }, () => invariant(sqlType.type) as never)
    .with({ type: 'BOOLEAN' }, () => 'Percentage')
    .with({ type: 'TINYINT' }, () => 'Uniform')
    .with({ type: 'SMALLINT' }, () => 'Uniform')
    .with({ type: 'INTEGER' }, () => 'Uniform')
    .with({ type: 'BIGINT' }, () => 'Uniform')
    .with({ type: 'DECIMAL' }, () => 'Uniform')
    .with({ type: 'REAL' }, () => 'Uniform')
    .with({ type: 'DOUBLE' }, () => 'Uniform')
    .with({ type: 'VARCHAR' }, () => 'Word')
    .with({ type: 'CHAR' }, () => 'Word')
    .with({ type: 'TIME' }, () => 'Uniform')
    .with({ type: 'TIMESTAMP' }, () => 'Uniform')
    .with({ type: 'DATE' }, () => 'Uniform')
    .with({ type: 'ARRAY' }, () => {
      invariant(sqlType.component !== null && sqlType.component !== undefined, 'Array type must have a component type')
      return getDefaultRngMethodName(sqlType.component)
    })
    .with({ type: 'BINARY' }, () => 'BINARY type not implemented')
    .with({ type: 'VARBINARY' }, () => 'VARBINARY type not implemented')
    .with({ type: { Interval: P._ } }, () => 'INTERVAL type not supported')
    .with({ type: 'STRUCT' }, () => 'STRUCT type not supported')
    .with({ type: 'NULL' }, () => 'NULL type not supported')
    .exhaustive()
}

// Given a title & SQL type, returns the corresponding generator method.
export const getRngMethodByName = (title: string, sqlType: ColumnType): IRngGenMethod | null => {
  const options = columnTypeToRngOptions(sqlType)
  return options.find(o => o.title === title) ?? null
}

// Returns the default generator method for a given SQL type.
export const getDefaultRngMethod = (sqlType: ColumnType): IRngGenMethod => {
  const title = getDefaultRngMethodName(sqlType)
  const method = getRngMethodByName(title, sqlType)
  invariant(method !== null, `Could not find default method for type ${sqlType.type}`)
  return method
}

// Transforms the list of generators from a base-type to a list with generators
// that work for the respective array type.
const transformToArrayGenerator = (type: ColumnType, generators: IRngGenMethod[]): IRngGenMethod[] => {
  if (type.component === undefined || type.component === null) {
    return generators
  }

  return generators.map(rgm => {
    invariant(type.component !== undefined && type.component !== null)

    let validationSchema = undefined
    if (rgm.validationSchema) {
      validationSchema = () => {
        invariant(rgm.validationSchema)
        return rgm.validationSchema(findBaseType(type.component as ColumnType))
      }
    }

    const generator = (ct: ColumnType, settings: Record<string, any> | undefined): SQLValueJS[] => {
      invariant(ct.component)
      const c = ct.component
      const { min, max } = numericRange(ct)
      const length = d3.randomInt.source(faker.number.float)(min.toNumber(), max.toNumber())()
      return Array.from({ length }, () => rgm.generator(c, settings))
    }

    return {
      ...rgm,
      generator,
      validationSchema
    }
  })
}

// Given a ColumType returns a list of applicable generator methods.
export const columnTypeToRngOptions = (type: ColumnType): IRngGenMethod[] => {
  invariant(type.type)
  return (
    match(type.type)
      .with('TINYINT', 'SMALLINT', 'INTEGER', () => INTEGER_GENERATORS)
      .with('BIGINT', () => BIGINTEGER_GENERATORS)
      .with('REAL', 'DOUBLE', () => FLOAT_GENERATORS)
      // There aren't any good random generator libraries for arbitrary
      // precision numbers. So we just use the same generators as FLOAT and
      // DOUBLE for now.
      .with('DECIMAL', () => DECIMAL_GENERATORS)
      .with('VARCHAR', 'CHAR', () => STRING_GENERATORS)
      .with('BOOLEAN', () => BOOLEAN_GENERATORS)
      .with('TIME', () => TIME_GENERATORS)
      .with('DATE', () => DATE_GENERATORS)
      .with('TIMESTAMP', () => TIMESTAMP_GENERATORS)
      .with('ARRAY', () => {
        invariant(type.component)
        return transformToArrayGenerator(type, columnTypeToRngOptions(type.component))
      })
      .with({ Interval: P._ }, 'BINARY', 'VARBINARY', 'STRUCT', 'NULL', () => UNSUPPORTED_TYPE_GENERATORS)
      .exhaustive()
      .map(({ generator, ...rng }) => ({
        ...rng,
        generator: (ct, settings) => generator(ct, settings)
      }))
  )
}

const UNSUPPORTED_TYPE_GENERATORS: IRngGenMethod[] = [
  {
    title: 'Constant',
    category: Categories.DEFAULT,
    generator: (ct, settings) => settings?.value ?? '',
    form_fields: () => [
      {
        sm: 4,
        component: TextField,
        props: {
          name: FieldNames.VALUE,
          type: 'string'
        }
      }
    ],
    validationSchema: () => {
      return va.object({
        value: va.string()
      })
    }
  }
]

// Generators for boolean.
const BOOLEAN_GENERATORS: IRngGenMethod[] = [
  {
    title: 'Constant',
    category: Categories.DEFAULT,
    generator: (ct, settings) => settings?.bool_const ?? false,
    form_fields: () => [
      {
        sm: 4,
        component: BooleanSwitch,
        props: { name: FieldNames.BOOL_CONST, label: 'Set to true' }
      }
    ],
    validationSchema: () => {
      return va.object({
        bool_const: va.boolean()
      })
    }
  },
  {
    title: 'Percentage',
    category: Categories.DISTRIBUTION,
    generator: (ct, settings) => faker.number.float() < (settings?.true_pct ?? 0.5),
    form_fields: () => [
      {
        sm: 4,
        component: NumberInput,
        props: {
          min: 0,
          max: 1,
          step: 0.01,
          name: FieldNames.TRUE_PCT,
          label: 'True [%]'
        }
      }
    ],
    validationSchema: () => {
      return va.object({
        true_pct: va.nonOptional(va.number([va.minValue(0), va.maxValue(1)]), 'Percentage required.')
      })
    }
  }
]

const rangeBigNumber = ({ min, max }: { min: BigNumber; max: BigNumber }) => [minBigNumber(min), maxBigNumber(max)]

const clampToRange = (ct: ColumnType, n: BigNumber) =>
  clampBigNumber(...(({ min, max }) => tuple(min, max))(numericRange(ct)), n)
const clampToScale = (ct: ColumnType, n: BigNumber) =>
  nonNull(ct.scale) ? n.decimalPlaces(ct.scale, BigNumber.ROUND_HALF_UP) : n

// Generic generators for integer (and floating point) numbers.
const NUMBER_GENERATORS: IRngGenMethod[] = [
  {
    title: 'Constant',
    category: Categories.DEFAULT,
    generator: (ct, settings) => {
      return new BigNumber(settings?.value || 0)
    },
    form_fields: () => [
      {
        sm: 4,
        component: BigNumberInput,
        props: { name: FieldNames.VALUE, label: 'Constant Value' }
      }
    ],
    validationSchema: ct => {
      const range = rangeBigNumber(numericRange(ct))
      return va.object({
        value: va.optional(bignumber(range))
      })
    }
  },
  {
    title: 'Normal',
    category: Categories.DISTRIBUTION,
    generator: (ct, props) =>
      clampToRange(
        ct,
        randomNormalBigNumber
          .source(faker.number.float)(props?.mu ?? new BigNumber(100), props?.sigma ?? new BigNumber(10))()
          .decimalPlaces(0, BigNumber.ROUND_FLOOR)
      ),
    form_fields: () => [
      {
        sm: 2,
        component: BigNumberInput,
        props: {
          fullWidth: true,
          name: FieldNames.MU,
          label: 'μ'
        }
      },
      {
        sm: 2,
        component: BigNumberInput,
        props: { fullWidth: true, name: FieldNames.SIGMA, label: 'σ', inputProps: { step: 0.1 } }
      }
    ],
    validationSchema: ct => {
      const range = rangeBigNumber(numericRange(ct))
      return va.object({
        mu: va.nonOptional(bignumber(range), 'Mu value is required.'),
        sigma: va.nonOptional(bignumber(range), 'Sigma is required.')
      })
    }
  },
  {
    title: 'Exponential',
    category: Categories.DISTRIBUTION,
    generator: (ct, props) => {
      return clampToRange(
        ct,
        randomExponentialBigNumber
          .source(faker.number.float)(props?.lambda ?? new BigNumber(0.1))()
          .decimalPlaces(0, BigNumber.ROUND_FLOOR)
      )
    },
    form_fields: () => [
      {
        sm: 2,
        // TODO: refactor to BigNumber input
        component: BigNumberInput,
        props: {
          fullWidth: true,
          name: FieldNames.LAMBDA,
          label: 'λ',
          inputProps: { step: 0.1 }
        }
      }
    ],
    validationSchema: () =>
      va.object({
        lambda: va.nonOptional(bignumber(), 'Lambda is required.')
      })
  }
]

const BIGINTEGER_GENERATORS: IRngGenMethod[] = NUMBER_GENERATORS.concat([
  {
    title: 'Uniform',
    category: Categories.DISTRIBUTION,
    generator: (ct, props) => {
      const convenientMin = 0
      const convenientMax = 100000
      const { min: typeMin, max: typeMax } = numericRange(ct)
      const min = ((props?.min as BigNumber | undefined) ?? BigNumber.max(convenientMin, typeMin)).decimalPlaces(
        0,
        BigNumber.ROUND_CEIL
      )
      const max = ((props?.max as BigNumber | undefined) ?? BigNumber.min(convenientMax, typeMax)).decimalPlaces(
        0,
        BigNumber.ROUND_FLOOR
      )
      return randomIntBigNumber.source(faker.number.float)(min, max)()
    },
    form_fields: ({ columntype: ct }) => [
      {
        sm: 2,
        component: BigNumberInput,
        props: {
          fullWidth: true,
          name: FieldNames.MIN,
          label: 'Minimum',
          ...numericRange(ct)
        }
      },
      {
        sm: 2,
        component: BigNumberInput,
        props: {
          fullWidth: true,
          name: FieldNames.MAX,
          label: 'Maximum',
          ...numericRange(ct)
        }
      }
    ],
    validationSchema: ct => {
      const range = rangeBigNumber(numericRange(ct))
      return va.object({
        min: bignumber(range),
        max: bignumber(range)
      })
    }
  }
])

const INTEGER_GENERATORS: IRngGenMethod[] = BIGINTEGER_GENERATORS.map(({ generator, ...rest }) => ({
  generator: (config, settings) => (generator(config, settings) as BigNumber).toNumber(),
  ...rest
}))

// Generators for floating point numbers.
//
// This 'inherits' all the integer generators.
const DECIMAL_GENERATORS: IRngGenMethod[] = NUMBER_GENERATORS.concat([
  {
    title: 'Uniform',
    category: Categories.DISTRIBUTION,
    generator: (ct, props?: { min?: BigNumber; max?: BigNumber }) => {
      const convenientMin = 0
      const convenientMax = 100
      const { min, max } = (({ min, max }) => ({ min: min.toNumber(), max: max.toNumber() }))(
        props?.min && props.max
          ? { min: props.min, max: props.max }
          : (({ min, max }) => ({
              min: BigNumber.max(convenientMin, min),
              max: BigNumber.min(convenientMax, max)
            }))(numericRange(ct))
      )
      // We use d3.randomUniform for generating floating point numbers as long
      // as the user set a range for the values.
      // TODO: refactor from casting to actual BigNumber implementation of randomUniform
      //
      // See also:
      // https://stackoverflow.com/questions/28461796/randomint-function-that-can-uniformly-handle-the-full-range-of-min-and-max-safe
      return clampToScale(ct, new BigNumber(d3.randomUniform.source(faker.number.float)(min, max)()))
    },
    form_fields: () => [
      {
        sm: 2,
        component: BigNumberInput,
        props: {
          fullWidth: true,
          name: FieldNames.MIN,
          label: 'Minimum'
        }
      },
      {
        sm: 2,
        component: BigNumberInput,
        props: { fullWidth: true, name: FieldNames.MAX, label: 'Maximum' }
      }
    ],
    validationSchema: ct => {
      const range = rangeBigNumber(numericRange(ct))
      return va.object({
        min: bignumber(range),
        max: bignumber(range)
      })
    }
  },
  {
    title: 'Beta',
    category: Categories.DISTRIBUTION,
    // TODO: refactor from casting to actual BigNumber implementation of randomBeta
    generator: (ct, props) => {
      return clampToScale(
        ct,
        clampToRange(
          ct,
          new BigNumber(
            d3.randomBeta.source(faker.number.float)(props?.alpha?.toNumber() ?? 1, props?.beta?.toNumber() ?? 1)()
          )
        )
      )
    },
    form_fields: () => [
      {
        sm: 2,
        component: BigNumberInput,
        props: {
          fullWidth: true,
          name: FieldNames.ALPHA,
          label: 'α',
          inputProps: { min: 0, step: 0.1 }
        }
      },
      {
        sm: 2,
        component: BigNumberInput,
        props: {
          fullWidth: true,
          name: FieldNames.BETA,
          label: 'β',
          inputProps: { min: 0, step: 0.1 }
        }
      }
    ],
    validationSchema: () =>
      va.object({
        alpha: va.nonOptional(bignumber([minBigNumber(new BigNumber(0))]), 'Constant value is required.'),
        beta: va.nonOptional(bignumber([minBigNumber(new BigNumber(0))]), 'Constant value is required.')
      })
  }
])

const FLOAT_GENERATORS: IRngGenMethod[] = DECIMAL_GENERATORS.map(({ generator, ...rest }) => ({
  generator: (config, settings) => (generator(config, settings) as BigNumber).toNumber(),
  ...rest
}))

const TIME_GENERATORS: IRngGenMethod[] = [
  {
    title: 'Constant',
    category: Categories.DEFAULT,
    generator: (ct, settings) => dayjs(settings?.time ? new Date(settings.time) : faker.date.anytime()),
    form_fields: field => [
      {
        sm: 3,
        component: TimePicker,
        props: {
          name: FieldNames.TIME,
          slotProps: { textField: { name: FieldNames.TIME } },
          label: 'Constant Time',
          views: ['hours', 'minutes', 'seconds'],
          minTime: dateTimeRange(field.columntype)[0],
          maxTime: dateTimeRange(field.columntype)[1]
        }
      }
    ]
  },
  {
    title: 'Uniform',
    category: Categories.DISTRIBUTION,
    generator: (ct, settings) => {
      const [min, max] = dateTimeRange(ct)
      const from = new Date(settings?.time ?? min)
      const to = new Date(settings?.time2 ?? max)

      // Ensure the DD-MM-YYYY portion of the date match before we generate a
      // random time
      from.setDate(to.getDate())
      from.setMonth(to.getMonth())
      from.setFullYear(to.getFullYear())

      return dayjs(faker.date.between({ from, to }))
    },
    // Replace form element with TimeRangePicker when added
    //
    // See also:
    // - https://mui.com/x/react-date-pickers/time-range-picker/
    // - https://github.com/mui/mui-x/issues/4460
    form_fields: field => [
      {
        sm: 3,
        component: TimePicker,
        props: {
          name: FieldNames.TIME,
          label: 'From',
          views: ['hours', 'minutes', 'seconds'],
          minTime: dateTimeRange(field.columntype)[0],
          maxTime: dateTimeRange(field.columntype)[1]
        }
      },
      {
        sm: 3,
        component: TimePicker,
        props: {
          name: FieldNames.TIME2,
          label: 'To',
          views: ['hours', 'minutes', 'seconds'],
          minTime: dateTimeRange(field.columntype)[0],
          maxTime: dateTimeRange(field.columntype)[1]
        }
      }
    ]
  }
]

const DATE_GENERATORS: IRngGenMethod[] = [
  {
    title: 'Constant',
    category: Categories.DEFAULT,
    generator: (ct, settings) => dayjs(settings?.time ? new Date(settings.time) : faker.date.anytime()),
    form_fields: field => [
      {
        sm: 3,
        component: DatePicker,
        props: {
          name: FieldNames.TIME,
          label: 'Constant Date',
          minDate: dateTimeRange(field.columntype)[0],
          maxDate: dateTimeRange(field.columntype)[1]
        }
      }
    ]
  },
  {
    title: 'Uniform',
    category: Categories.DISTRIBUTION,
    generator: (ct, settings) => {
      const [min, max] = dateTimeRange(ct)
      return dayjs(
        faker.date.between({
          from: new Date(settings?.date_range[0] ?? min),
          to: new Date(settings?.date_range[1] ?? max)
        })
      )
    },
    form_fields: field => [
      {
        sm: 6,
        component: DateRangePicker,
        props: {
          name: FieldNames.DATE_RANGE,
          localeText: { start: 'From', end: 'To' },
          minDate: dateTimeRange(field.columntype)[0],
          maxDate: dateTimeRange(field.columntype)[1]
        }
      }
    ]
  }
]

const TIMESTAMP_GENERATORS: IRngGenMethod[] = [
  {
    title: 'Constant',
    category: Categories.DEFAULT,
    generator: (ct, settings) => dayjs(new Date(settings?.time)),
    form_fields: field => [
      {
        sm: 4,
        component: DateTimePicker,
        props: {
          name: FieldNames.TIME,
          minDate: dateTimeRange(field.columntype)[0],
          maxDate: dateTimeRange(field.columntype)[1]
        }
      }
    ]
  },
  {
    title: 'Uniform',
    category: Categories.DISTRIBUTION,
    generator: (ct, settings) => {
      const [min, max] = dateTimeRange(ct)
      return dayjs(
        faker.date.between({
          from: new Date(settings?.date_range[0] ?? min),
          to: new Date(settings?.date_range[1] ?? max)
        })
      )
    },
    // Replace with DateTimeRangePicker when ready
    // See also:
    // - https://mui.com/x/react-date-pickers/date-time-range-picker/
    form_fields: field => [
      {
        sm: 6,
        component: MultiInputDateTimeRangeField,
        props: {
          name: FieldNames.DATE_RANGE,
          slotProps: {
            textField: ({ position }: { position: string }) => ({
              label: position === 'start' ? 'From' : 'To'
            })
          },
          minDateTime: dateTimeRange(field.columntype)[0],
          maxDateTime: dateTimeRange(field.columntype)[1]
        }
      }
    ]
  }
]

// Generators for string types.
const STRING_GENERATORS: IRngGenMethod[] = [
  // Basic Strings
  {
    title: 'Word',
    category: Categories.RAND_STRINGS,
    generator: () => faker.lorem.word()
  },
  {
    title: 'Slug',
    category: Categories.RAND_STRINGS,
    generator: () => faker.lorem.slug()
  },
  {
    title: 'Words',
    category: Categories.RAND_STRINGS,
    generator: () => faker.lorem.words()
  },
  {
    title: 'Lines',
    category: Categories.RAND_STRINGS,
    generator: () => faker.lorem.lines()
  },
  {
    title: 'Paragraph',
    category: Categories.RAND_STRINGS,
    generator: () => faker.lorem.paragraph()
  },
  {
    title: 'Paragraphs',
    category: Categories.RAND_STRINGS,
    generator: () => faker.lorem.paragraphs()
  },
  {
    title: 'Sentence',
    category: Categories.RAND_STRINGS,
    generator: () => faker.lorem.sentence()
  },
  {
    title: 'Sentences',
    category: Categories.RAND_STRINGS,
    generator: () => faker.lorem.sentences()
  },
  {
    title: 'Text',
    category: Categories.RAND_STRINGS,
    generator: () => faker.lorem.text()
  },
  // Person
  {
    title: 'Prefix',
    category: Categories.PERSON,
    generator: () => faker.person.prefix()
  },
  {
    title: 'First Name',
    category: Categories.PERSON,
    generator: () => faker.person.firstName()
  },
  {
    title: 'Middle Name',
    category: Categories.PERSON,
    generator: () => faker.person.middleName()
  },
  {
    title: 'Last Name',
    category: Categories.PERSON,
    generator: () => faker.person.lastName()
  },
  {
    title: 'Full Name',
    category: Categories.PERSON,
    generator: () => faker.person.fullName()
  },
  {
    title: 'Suffix',
    category: Categories.PERSON,
    generator: () => faker.person.suffix()
  },
  {
    title: 'Bio',
    category: Categories.PERSON,
    generator: () => faker.person.bio()
  },
  {
    title: 'Country',
    category: Categories.LOCATION,
    generator: () => faker.location.country()
  },
  {
    title: 'Country Code',
    category: Categories.LOCATION,
    generator: () => faker.location.countryCode()
  },
  {
    title: 'State',
    category: Categories.LOCATION,
    generator: () => faker.location.state()
  },
  {
    title: 'County',
    category: Categories.LOCATION,
    generator: () => faker.location.county()
  },
  {
    title: 'City',
    category: Categories.LOCATION,
    generator: () => faker.location.city()
  },
  {
    title: 'Street',
    category: Categories.LOCATION,
    generator: () => faker.location.street()
  },
  {
    title: 'Street Address',
    category: Categories.LOCATION,
    generator: () => faker.location.streetAddress()
  },
  {
    title: 'Secondary Address',
    category: Categories.LOCATION,
    generator: () => faker.location.secondaryAddress()
  },
  {
    title: 'Time Zone',
    category: Categories.LOCATION,
    generator: () => faker.location.timeZone()
  },
  {
    title: 'ZIP Code',
    category: Categories.LOCATION,
    generator: () => faker.location.zipCode()
  },
  {
    title: 'Phone number',
    category: Categories.PHONE_NUMBER,
    generator: () => faker.phone.number()
  },
  {
    title: 'IMEI',
    category: Categories.PHONE_NUMBER,
    generator: () => faker.phone.imei()
  },
  {
    title: 'Account Name',
    category: Categories.FINANCE,
    generator: () => faker.finance.accountName()
  },
  {
    title: 'Account Number',
    category: Categories.FINANCE,
    generator: () => faker.finance.accountNumber()
  },
  {
    title: 'SWIFT/BIC Code',
    category: Categories.FINANCE,
    generator: () => faker.finance.bic()
  },
  {
    title: 'IBAN',
    category: Categories.FINANCE,
    generator: () => faker.finance.iban()
  },
  {
    title: 'Credit Card Number',
    category: Categories.FINANCE,
    generator: () => faker.finance.creditCardNumber()
  },
  {
    title: 'Credit Card CVV',
    category: Categories.FINANCE,
    generator: () => faker.finance.creditCardCVV()
  },
  {
    title: 'Credit Card Issuer',
    category: Categories.FINANCE,
    generator: () => faker.finance.creditCardIssuer()
  },
  {
    title: 'Currency Code',
    category: Categories.FINANCE,
    generator: () => faker.finance.currencyCode()
  },
  {
    title: 'Currency Name',
    category: Categories.FINANCE,
    generator: () => faker.finance.currencyName()
  },
  {
    title: 'Currency Symbol',
    category: Categories.FINANCE,
    generator: () => faker.finance.currencySymbol()
  },
  {
    title: 'Routing Number',
    category: Categories.FINANCE,
    generator: () => faker.finance.routingNumber()
  },
  {
    title: 'Transaction Description',
    category: Categories.FINANCE,
    generator: () => faker.finance.transactionDescription()
  },
  {
    title: 'Transaction Type',
    category: Categories.FINANCE,
    generator: () => faker.finance.transactionType()
  },
  {
    title: 'Department',
    category: Categories.COMMERCE,
    generator: () => faker.commerce.department()
  },
  {
    title: 'Product',
    category: Categories.COMMERCE,
    generator: () => faker.commerce.product()
  },
  {
    title: 'Product Name',
    category: Categories.COMMERCE,
    generator: () => faker.commerce.productMaterial()
  },
  {
    title: 'Product description',
    category: Categories.COMMERCE,
    generator: () => faker.commerce.productDescription()
  },
  {
    title: 'Product material',
    category: Categories.COMMERCE,
    generator: () => faker.commerce.productMaterial()
  },
  {
    title: 'Username',
    category: Categories.COMPUTER,
    generator: () => faker.internet.displayName()
  },
  {
    title: 'Domain Name',
    category: Categories.COMPUTER,
    generator: () => faker.internet.domainName()
  },
  {
    title: 'URL',
    category: Categories.COMPUTER,
    generator: () => faker.internet.url()
  },
  {
    title: 'User Agent',
    category: Categories.COMPUTER,
    generator: () => faker.internet.userAgent()
  },
  {
    title: 'Email',
    category: Categories.COMPUTER,
    generator: () => faker.internet.email()
  },
  {
    title: 'HTTP Method',
    category: Categories.COMPUTER,
    generator: () => faker.internet.httpMethod()
  },
  {
    title: 'HTTP Status Code',
    category: Categories.COMPUTER,
    generator: () => faker.internet.httpStatusCode().toString()
  },
  {
    title: 'IPv4 Address',
    category: Categories.COMPUTER,
    generator: () => faker.internet.ipv4()
  },
  {
    title: 'IPv6 Address',
    category: Categories.COMPUTER,
    generator: () => faker.internet.ipv6()
  },
  {
    title: 'MAC Address',
    category: Categories.COMPUTER,
    generator: () => faker.internet.mac()
  },
  {
    title: 'Password',
    category: Categories.COMPUTER,
    generator: () => faker.internet.password()
  },
  {
    title: 'DB Column Name',
    category: Categories.COMPUTER,
    generator: () => faker.database.column()
  },
  {
    title: 'DB Column Type',
    category: Categories.COMPUTER,
    generator: () => faker.database.type()
  },
  {
    title: 'Collation',
    category: Categories.COMPUTER,
    generator: () => faker.database.collation()
  },
  {
    title: 'Object Id',
    category: Categories.COMPUTER,
    generator: () => faker.database.mongodbObjectId()
  },
  {
    title: 'UUID v4',
    category: Categories.COMPUTER,
    generator: () => faker.string.uuid()
  },
  {
    title: 'Git Branch',
    category: Categories.COMPUTER,
    generator: () => faker.git.branch()
  },
  {
    title: 'Git Commit Message',
    category: Categories.COMPUTER,
    generator: () => faker.git.commitMessage()
  },
  {
    title: 'Git Commit SHA',
    category: Categories.COMPUTER,
    generator: () => faker.git.commitSha()
  },
  {
    title: 'File Name',
    category: Categories.COMPUTER,
    generator: () => faker.system.commonFileName()
  },
  {
    title: 'File Extension',
    category: Categories.COMPUTER,
    generator: () => faker.system.commonFileExt()
  },
  {
    title: 'File Type',
    category: Categories.COMPUTER,
    generator: () => faker.system.commonFileType()
  },
  {
    title: 'Directory Path',
    category: Categories.COMPUTER,
    generator: () => faker.system.directoryPath()
  },
  {
    title: 'File Path',
    category: Categories.COMPUTER,
    generator: () => faker.system.filePath()
  },
  {
    title: 'MIME Type',
    category: Categories.COMPUTER,
    generator: () => faker.system.mimeType()
  },
  {
    title: 'Semantic Version',
    category: Categories.COMPUTER,
    generator: () => faker.system.semver()
  },
  {
    title: 'Airplane',
    category: Categories.TRAVEL,
    generator: () => faker.airline.airplane().name
  },
  {
    title: 'Airplane Code',
    category: Categories.TRAVEL,
    generator: () => faker.airline.airplane().iataTypeCode
  },
  {
    title: 'Airlane',
    category: Categories.TRAVEL,
    generator: () => faker.airline.airline().name
  },
  {
    title: 'Airlane Code',
    category: Categories.TRAVEL,
    generator: () => faker.airline.airline().iataCode
  },
  {
    title: 'Airport',
    category: Categories.TRAVEL,
    generator: () => faker.airline.airport().name
  },
  {
    title: 'Airport Code',
    category: Categories.TRAVEL,
    generator: () => faker.airline.airport().iataCode
  },
  {
    title: 'Flight number',
    category: Categories.TRAVEL,
    generator: () => faker.airline.flightNumber()
  },
  {
    title: 'Record Locator',
    category: Categories.TRAVEL,
    generator: () => faker.airline.recordLocator()
  },
  {
    title: 'Airplane Seat',
    category: Categories.TRAVEL,
    generator: () => faker.airline.seat()
  },
  {
    title: 'Vehicle',
    category: Categories.TRAVEL,
    generator: () => faker.vehicle.vehicle()
  },
  {
    title: 'Vehicle Model',
    category: Categories.TRAVEL,
    generator: () => faker.vehicle.model()
  },
  {
    title: 'Vehicle Manufacturer',
    category: Categories.TRAVEL,
    generator: () => faker.vehicle.manufacturer()
  },
  {
    title: 'Vehicle Type',
    category: Categories.TRAVEL,
    generator: () => faker.vehicle.type()
  },
  {
    title: 'VIN',
    category: Categories.TRAVEL,
    generator: () => faker.vehicle.vin()
  },
  {
    title: 'VRM',
    category: Categories.TRAVEL,
    generator: () => faker.vehicle.vrm()
  },
  {
    title: 'Bicycle',
    category: Categories.TRAVEL,
    generator: () => faker.vehicle.bicycle()
  },
  {
    title: 'Color',
    category: Categories.PHYSICAL,
    generator: () => faker.color.human()
  },
  {
    title: 'RGB Color',
    category: Categories.PHYSICAL,
    generator: () => faker.color.rgb()
  },
  {
    title: 'Chemical Element Name',
    category: Categories.PHYSICAL,
    generator: () => faker.science.chemicalElement().name
  },
  {
    title: 'Chemical Element Symbol',
    category: Categories.PHYSICAL,
    generator: () => faker.science.chemicalElement().symbol
  },
  {
    title: 'Unit Name',
    category: Categories.PHYSICAL,
    generator: () => faker.science.unit().name
  },
  {
    title: 'Unit Symbol',
    category: Categories.PHYSICAL,
    generator: () => faker.science.unit().symbol
  }
].map(g => ({
  ...g,
  generator: ({ type, precision }) => {
    const string = g.generator()
    return match({ type, precision })
      .with(
        {
          type: 'VARCHAR',
          precision: P.when(value => (value ?? -1) >= 0)
        },
        ({ precision }) => string.substring(0, precision!)
      )
      .with(
        {
          type: 'CHAR',
          precision: P.when(value => (value ?? -1) >= 0)
        },
        ({ precision }) => string.substring(0, precision!).padEnd(precision!)
      )
      .otherwise(() => string)
  }
}))
