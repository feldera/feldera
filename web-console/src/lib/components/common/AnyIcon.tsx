import Image from 'next/image'
import { CSSProperties } from 'react'

import { Icon, IconProps } from '@iconify/react'

export const AnyIcon = (props: IconProps & { icon: string | { src: string } }) =>
  typeof props.icon === 'object' && typeof props.icon.src === 'string' && props.icon.src[0] === '/' ? (
    <Image src={props.icon as any} style={{ ...props, ...props.style } as CSSProperties} alt='An icon'></Image>
  ) : (
    <Icon {...props}></Icon>
  )
