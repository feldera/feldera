import { BigNumberInput } from '$lib/components/input/BigNumberInput'
import { NumberInput } from '$lib/components/input/NumberInput'

import { expect, MountResult, test } from '@playwright/experimental-ct-react'

const testKeyStrokes = async (input: MountResult) => {
  await input.click()
  await input.pressSequentially('10')
  await expect(input).toHaveScreenshot('1-1.png')
  await input.pressSequentially('0')
  await expect(input).toHaveScreenshot('1-2.png')
  await input.pressSequentially('00')
  await expect(input).toHaveScreenshot('1-3.png')
  await input.press('Backspace')
  await input.press('Delete')
  await input.press('ArrowLeft')
  await input.press('Delete')
  await expect(input).toHaveScreenshot('1-4.png')
  await input.press('Control+A')
  await input.press('Delete')
  await input.pressSequentially('98')
  await expect(input).toHaveScreenshot('1-5.png')
  await input.press('Shift+ArrowLeft')
  await input.press('Control+X')
  await expect(input).toHaveScreenshot('1-6.png')
  await input.press('Period')
  await input.press('Period')
  await expect(input).toHaveScreenshot('1-7.png')
  await input.press('Control+V')
  await input.press('Period')
  await expect(input).toHaveScreenshot('1-8.png')
  await input.press('4')
  await expect(input).toHaveScreenshot('1-9.png')
  await input.press('ArrowLeft')
  await input.press('Control+V')
  await expect(input).toHaveScreenshot('1-10.png')
  await input.press('ArrowLeft')
  await input.press('7')
  await expect(input).toHaveScreenshot('1-11.png')
  await input.press('Home')
  await input.press('6')
  await expect(input).toHaveScreenshot('1-12.png')
}

test('NumberInput: Test key strokes', async ({ mount }) => {
  await testKeyStrokes(await mount(<NumberInput min={100} max={10000} sx={{ width: 200 }}></NumberInput>))
})

test('BigNumberInput: Test key strokes', async ({ mount }) => {
  await testKeyStrokes(await mount(<BigNumberInput min={100} max={10000} sx={{ width: 200 }}></BigNumberInput>))
})
