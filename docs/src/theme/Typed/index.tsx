import React, { useRef, useEffect } from 'react'
import Typed, { type TypedOptions } from 'typed.js'

const ReactTyped = (props: TypedOptions) => {
  const typeTarget = useRef<HTMLSpanElement>(null)
  const { strings, typeSpeed } = props

  useEffect(() => {
    if (!typeTarget.current) {
      return
    }

    const typed = new Typed(typeTarget.current, {
      strings: strings,
      typeSpeed: typeSpeed ?? 40
    })

    return () => {
      typed.destroy()
    }
  }, [strings, typeSpeed])

  return <span ref={typeTarget} />
}

export default ReactTyped
