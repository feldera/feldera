import React, { useRef } from 'react'

import * as THREE from 'three'
import { Canvas, useFrame, ThreeElements } from '@react-three/fiber'

function FlowingLinesShader(props: ThreeElements['mesh']) {
  const ref = useRef<THREE.Mesh>(null!)
  const tuniform = {
    iTime: { type: 'f', value: 0.0 },
    iResolution: { type: 'v2', value: new THREE.Vector2(1, 1) }
  }

  useFrame((state, delta) => (tuniform.iTime.value += delta))

  const vshader = `
  varying vec2 vUv;
  void main() {
    vUv = uv;
    vec4 mvPosition = modelViewMatrix * vec4(position, 1.0);
    gl_Position = projectionMatrix * mvPosition;
  }
  `

  const fshader = `
varying vec2 vUv;
uniform vec2 iResolution;
uniform float iTime;
#define WAVES 4.0

// discontinuous pseudorandom uniformly distributed in [-0.5, +0.5]^3
// Copyright © 2013 Nikita Miropolskiy, MIT License
vec3 random3(vec3 c) {
	float j = 4096.0*sin(dot(c,vec3(17.0, 59.4, 15.0)));
	vec3 r;
	r.z = fract(512.0*j);
	j *= .125;
	r.x = fract(512.0*j);
	j *= .125;
	r.y = fract(512.0*j);
	return r-0.5;
}

/* skew constants for 3d simplex functions */
const float F3 =  0.3333333;
const float G3 =  0.1666667;

// 3d simplex noise
// Copyright © 2013 Nikita Miropolskiy, MIT License
float simplex3d(vec3 p) {
	 /* 1. find current tetrahedron T and it's four vertices */
	 /* s, s+i1, s+i2, s+1.0 - absolute skewed (integer) coordinates of T vertices */
	 /* x, x1, x2, x3 - unskewed coordinates of p relative to each of T vertices*/

	 /* calculate s and x */
	 vec3 s = floor(p + dot(p, vec3(F3)));
	 vec3 x = p - s + dot(s, vec3(G3));

	 /* calculate i1 and i2 */
	 vec3 e = step(vec3(0.0), x - x.yzx);
	 vec3 i1 = e*(1.0 - e.zxy);
	 vec3 i2 = 1.0 - e.zxy*(1.0 - e);

	 /* x1, x2, x3 */
	 vec3 x1 = x - i1 + G3;
	 vec3 x2 = x - i2 + 2.0*G3;
	 vec3 x3 = x - 1.0 + 3.0*G3;

	 /* 2. find four surflets and store them in d */
	 vec4 w, d;

	 /* calculate surflet weights */
	 w.x = dot(x, x);
	 w.y = dot(x1, x1);
	 w.z = dot(x2, x2);
	 w.w = dot(x3, x3);

	 /* w fades from 0.6 at the center of the surflet to 0.0 at the margin */
	 w = max(0.6 - w, 0.0);

	 /* calculate surflet components */
	 d.x = dot(random3(s), x);
	 d.y = dot(random3(s + i1), x1);
	 d.z = dot(random3(s + i2), x2);
	 d.w = dot(random3(s + 1.0), x3);

	 /* multiply d by w^4 */
	 w *= w;
	 w *= w;
	 d *= w;

	 /* 3. return the sum of the four surflets */
	 return dot(d, vec4(52.0));
}

float smooth_circle(vec2 position, float radius) {
    return 1.0 - smoothstep(0.0, radius, length(position));
}


float circle(float radius, vec2 center, vec2 uv) {
	float d = distance(center, uv);
    return 1.0 - smoothstep(radius, radius+.002, d);
}

float Y(float x, float time, float freq, float i) {
    return sin(x * 10.0 - time) * cos(x * 2.0) * freq * 0.4 * ((i + 1.0) / WAVES) * simplex3d(vec3(freq)) * smoothstep(0.0, 1.0, x);
}

void main() {
	vec2 uv = -1.0 + 2.0 * vUv.xy / iResolution.xy;

	float time = iTime * 1.0;
	vec4 color = vec4(0.0, 18./255., 31./255., 1.);

	for (float i=1.0; i<WAVES; i++) {
		float freq = float((i+6.)) * .2 + simplex3d(vec3(i / WAVES)) + random3(vec3(i / WAVES)).x;
		vec2 p = vec2(uv);

		p.x += i * 0.04 + freq;
		p.y += Y(p.x, time, freq, i);

		float intensity = abs(0.01 / p.y) * clamp(freq, 0.35, .6);

        float circleX = mod(i + iTime * .5, 4.0)-1.;
        vec2 circlePosition = vec2(circleX, -Y(p.x, time, freq, i));
        float circle = circle(.01, uv, circlePosition);

        color += vec4(circle) * vec4(0.0, 255./255., 255./255., 1.);
		color += smoothstep(0.0, .8, vec4(.25 * intensity * (i / 5.0), 0.5 * intensity, 1. * intensity, 1.));
	}

	gl_FragColor = vec4(color);
}
`

  return (
    <mesh geometry={new THREE.PlaneGeometry(window.outerWidth, 394, 1, 1)} {...props} ref={ref}>
      <shaderMaterial vertexShader={vshader} fragmentShader={fshader} uniforms={tuniform} side={THREE.DoubleSide} />
    </mesh>
  )
}

export function FlowingLines() {
  const camera = new THREE.OrthographicCamera(
    -1, // left
    1, // right
    1, // top
    -1, // bottom
    -1, // near,
    1 // far
  )
  camera.lookAt(0, 0, 0)

  return (
    <Canvas camera={camera}>
      <pointLight position={[10, 10, 10]} />
      <FlowingLinesShader position={[0, -0, 0]} />
    </Canvas>
  )
}
