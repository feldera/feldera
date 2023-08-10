// Contains a lists of random generators for every SQL type, each random
// generator may have custom generation and validation methods.

import { getRandomDate } from '$lib/functions/common/date'
import { dateTimeRange, findBaseType, typeRange } from '$lib/types/ddl'
import { ColumnType, Field } from '$lib/types/manager'
import assert from 'assert'
import * as d3 from 'd3-random'
import dayjs, { Dayjs } from 'dayjs'
import { match } from 'ts-pattern'
import * as yup from 'yup'

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

type NestedArray<T> = T | NestedArray<T>[]
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
  generator: (
    config: ColumnType,
    settings?: Partial<Record<FieldNames, any>>
  ) =>
    | boolean
    | number
    | string
    | Dayjs
    | NestedArray<boolean>
    | NestedArray<number>
    | NestedArray<string>
    | NestedArray<Dayjs>
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
    props:
      | AddFieldName<TextFieldProps>
      | AddFieldName<SwitchProps>
      | AddFieldName<TimePickerProps<Date>>
      | AddFieldName<DatePickerProps<Date>>
      | AddFieldName<DateRangePickerProps<Date>>
      | AddFieldName<DateTimePickerProps<Date>>
      | AddFieldName<MultiInputDateTimeRangeFieldProps<Date>>
  }[]
  // A validation schema for the form fields to display errors in case an
  // invalid value is entered in a form.
  validationSchema?: (field: ColumnType) => yup.ObjectSchema<any>
}

const getDefaultRngMethodName = (sqlType: ColumnType): string =>
  match(sqlType)
    .with({ type: 'BOOLEAN' }, () => 'Percentage')
    .with({ type: 'TINYINT' }, () => 'Uniform')
    .with({ type: 'SMALLINT' }, () => 'Uniform')
    .with({ type: 'INTEGER' }, () => 'Uniform')
    .with({ type: 'BIGINT' }, () => 'Uniform')
    .with({ type: 'DECIMAL' }, () => 'Uniform')
    .with({ type: 'FLOAT' }, () => 'Uniform')
    .with({ type: 'DOUBLE' }, () => 'Uniform')
    .with({ type: 'VARCHAR' }, () => 'Word')
    .with({ type: 'CHAR' }, () => 'Word')
    .with({ type: 'TIME' }, () => 'Uniform')
    .with({ type: 'TIMESTAMP' }, () => 'Uniform')
    .with({ type: 'DATE' }, () => 'Uniform')
    .with({ type: 'GEOMETRY' }, () => 'Uniform')
    .with({ type: 'ARRAY' }, () => {
      assert(sqlType.component !== null && sqlType.component !== undefined, 'Array type must have a component type')
      return getDefaultRngMethodName(sqlType.component)
    })
    .otherwise(() => 'Constant')

// Given a title & SQL type, returns the corresponding generator method.
export const getRngMethodByName = (title: string, sqlType: ColumnType): IRngGenMethod | null => {
  const options = columnTypeToRngOptions(sqlType)
  return options.find(o => o.title === title) ?? null
}

// Returns the default generator method for a given SQL type.
export const getDefaultRngMethod = (sqlType: ColumnType): IRngGenMethod => {
  const title = getDefaultRngMethodName(sqlType)
  const method = getRngMethodByName(title, sqlType)
  assert(method !== null, `Could not find default method for type ${sqlType.type}`)
  return method
}

// Transforms the list of generators from a base-type to a list with generators
// that work for the respective array type.
const transformToArrayGenerator = (type: ColumnType, generators: IRngGenMethod[]): IRngGenMethod[] => {
  if (type.component === undefined || type.component === null) {
    return generators
  }

  return generators.map(rgm => {
    assert(type.component !== undefined && type.component !== null)

    let validationSchema = undefined
    if (rgm.validationSchema) {
      validationSchema = () => {
        assert(rgm.validationSchema)
        return rgm.validationSchema(findBaseType(type.component as ColumnType))
      }
    }

    const generator = (ct: ColumnType, settings: Record<string, any> | undefined): NestedArray<any> => {
      const [min, max] = typeRange(ct)
      const length = d3.randomInt(min, max)()
      const value = []
      for (let i = 0; i < length; i++) {
        assert(ct.component !== undefined && ct.component !== null)
        value.push(rgm.generator(ct.component, settings))
      }
      return value
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
  return (
    match(type)
      .with(
        { type: 'TINYINT' },
        { type: 'SMALLINT' },
        { type: 'INTEGER' },
        { type: 'BIGINT' },
        () => INTEGER_GENERATORS
      )
      .with({ type: 'FLOAT' }, { type: 'DOUBLE' }, () => FLOAT_GENERATORS)
      // There aren't any good random generator libraries for arbitrary
      // precision numbers. So we just use the same generators as FLOAT and
      // DOUBLE for now.
      .with({ type: 'DECIMAL' }, () => FLOAT_GENERATORS)
      .with({ type: 'VARCHAR' }, { type: 'TEXT' }, { type: 'CHAR' }, () => STRING_GENERATORS)
      .with({ type: 'BOOLEAN' }, () => BOOLEAN_GENERATORS)
      .with({ type: 'TIME' }, () => TIME_GENERATORS)
      .with({ type: 'DATE' }, () => DATE_GENERATORS)
      .with({ type: 'TIMESTAMP' }, () => TIMESTAMP_GENERATORS)
      .with({ type: 'ARRAY' }, () => {
        assert(type.component !== undefined && type.component !== null)
        return transformToArrayGenerator(type, columnTypeToRngOptions(type.component))
      })
      .otherwise(() => {
        throw new Error('unsupported type for RNG')
      })
  )
}

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
      return yup.object().shape({
        bool_const: yup.bool()
      })
    }
  },
  {
    title: 'Percentage',
    category: Categories.DISTRIBUTION,
    generator: (ct, settings) => Math.random() < (settings?.true_pct ?? 0.5),
    form_fields: () => [
      {
        sm: 4,
        component: TextField,
        props: {
          name: FieldNames.TRUE_PCT,
          label: 'True [%]',
          type: 'number',
          inputProps: { min: 0, max: 1, step: 0.01 }
        }
      }
    ],
    validationSchema: () => {
      return yup.object().shape({
        true_pct: yup.number().min(0).max(1).required('Percentage required.')
      })
    }
  }
]

// Generic generators for integer (and floating point) numbers.
const NUMBER_GENERATORS: IRngGenMethod[] = [
  {
    title: 'Constant',
    category: Categories.DEFAULT,
    generator: (ct, settings) => Number(settings?.value) || 0,
    form_fields: () => [
      {
        sm: 4,
        component: TextField,
        props: { name: FieldNames.VALUE, label: 'Constant Value', type: 'number' }
      }
    ],
    validationSchema: ct => {
      const [min, max] = typeRange(ct)
      return yup.object().shape({
        value: yup.number().min(min).max(max).required('Constant value is required.')
      })
    }
  },
  {
    title: 'Normal',
    category: Categories.DISTRIBUTION,
    generator: (ct, props) => Math.floor(d3.randomNormal(props?.mu ?? 100, props?.sigma ?? 10)()),
    form_fields: () => [
      {
        sm: 2,
        component: TextField,
        props: {
          fullWidth: true,
          type: 'number',
          name: FieldNames.MU,
          label: 'μ'
        }
      },
      {
        sm: 2,
        component: TextField,
        props: { fullWidth: true, type: 'number', name: FieldNames.SIGMA, label: 'σ', inputProps: { step: 0.1 } }
      }
    ],
    validationSchema: ct => {
      const [min, max] = typeRange(ct)
      return yup.object().shape({
        mu: yup.number().min(min).max(max).required('Mu value is required.'),
        sigma: yup.number().min(min).max(max).required('Sigma is required.')
      })
    }
  },
  {
    title: 'Exponential',
    category: Categories.DISTRIBUTION,
    generator: (ct, props) => Math.floor(d3.randomExponential(props?.lambda || 0.1)()),
    form_fields: () => [
      {
        sm: 2,
        component: TextField,
        props: {
          fullWidth: true,
          type: 'number',
          name: FieldNames.LAMBDA,
          label: 'λ',
          inputProps: { step: 0.1 }
        }
      }
    ],
    validationSchema: () =>
      yup.object().shape({
        lambda: yup.number().required('Lambda is required.')
      })
  }
]

const INTEGER_GENERATORS: IRngGenMethod[] = NUMBER_GENERATORS.concat([
  {
    title: 'Uniform',
    category: Categories.DISTRIBUTION,
    generator: (ct, props) => {
      const [typeMin, typeMax] = typeRange(ct)
      const min = Math.ceil(props?.min ?? typeMin)
      const max = Math.floor(props?.max ?? typeMax)
      return d3.randomInt(min, max)()
    },
    form_fields: () => [
      {
        sm: 2,
        component: TextField,
        props: {
          fullWidth: true,
          type: 'number',
          name: FieldNames.MIN,
          label: 'Minimum'
        }
      },
      {
        sm: 2,
        component: TextField,
        props: { fullWidth: true, type: 'number', name: FieldNames.MAX, label: 'Maximum' }
      }
    ],
    validationSchema: ct => {
      const [min, max] = typeRange(ct)
      return yup.object().shape({
        min: yup.number().min(min).max(max),
        max: yup.number().min(min).max(max)
      })
    }
  }
])

// Generators for floating point numbers.
//
// This 'inherits' all the integer generators.
const FLOAT_GENERATORS: IRngGenMethod[] = NUMBER_GENERATORS.concat([
  {
    title: 'Uniform',
    category: Categories.DISTRIBUTION,
    generator: (ct, props) => {
      if (props?.min && props.max) {
        // We use d3.randomUniform for generating floating point numbers as long
        // as the user set a range for the values.
        const min = props?.min
        const max = props?.max
        return d3.randomUniform(min, max)()
      } else {
        // Because it's surprisingly difficult to generate floating point
        // numbers with a max range of -Inf..Inf, we just default to 0..1 for
        // this one.
        //
        // See also:
        // https://stackoverflow.com/questions/28461796/randomint-function-that-can-uniformly-handle-the-full-range-of-min-and-max-safe
        return d3.randomUniform()()
      }
    },
    form_fields: () => [
      {
        sm: 2,
        component: TextField,
        props: {
          fullWidth: true,
          type: 'number',
          name: FieldNames.MIN,
          label: 'Minimum'
        }
      },
      {
        sm: 2,
        component: TextField,
        props: { fullWidth: true, type: 'number', name: FieldNames.MAX, label: 'Maximum' }
      }
    ],
    validationSchema: ct => {
      const [min, max] = typeRange(ct)
      return yup.object().shape({
        min: yup.number().min(min).max(max),
        max: yup.number().min(min).max(max)
      })
    }
  },
  {
    title: 'Beta',
    category: Categories.DISTRIBUTION,
    generator: (ct, props) => d3.randomBeta(props?.alpha ?? 1, props?.beta ?? 1)(),
    form_fields: () => [
      {
        sm: 2,
        component: TextField,
        props: {
          fullWidth: true,
          type: 'number',
          name: FieldNames.ALPHA,
          label: 'α',
          inputProps: { min: 0, step: 0.1 }
        }
      },
      {
        sm: 2,
        component: TextField,
        props: {
          fullWidth: true,
          type: 'number',
          name: FieldNames.BETA,
          label: 'β',
          inputProps: { min: 0, step: 0.1 }
        }
      }
    ],
    validationSchema: () =>
      yup.object().shape({
        alpha: yup.number().min(0).required('Constant value is required.'),
        beta: yup.number().min(0).required('Constant value is required.')
      })
  }
])

const TIME_GENERATORS: IRngGenMethod[] = [
  {
    title: 'Constant',
    category: Categories.DEFAULT,
    generator: (ct, settings) => dayjs(new Date(settings?.time)),
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

      return dayjs(getRandomDate(from, to))
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
    generator: (ct, settings) => dayjs(new Date(settings?.time)),
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
      return dayjs(getRandomDate(new Date(settings?.date_range[0] ?? min), new Date(settings?.date_range[1] ?? max)))
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
      return dayjs(getRandomDate(new Date(settings?.date_range[0] ?? min), new Date(settings?.date_range[1] ?? max)))
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
]
