import {fromEvent} from 'rxjs'
import {map, pairwise, switchMap, takeUntil, withLatestFrom, startWith} from 'rxjs/operators'

const canvas = document.querySelector('canvas')
const range = document.getElementById('range')
const color = document.getElementById('color')

const ctx = canvas.getContext('2d')
const rect = canvas.getBoundingClientRect()
const scale = window.devicePixelRatio

canvas.width = rect.width * scale
canvas.height = rect.height * scale
ctx.scale(scale, scale)
//streams
const mouseMove$ = fromEvent(canvas, 'mousemove')
const mouseDown$ = fromEvent(canvas, 'mousedown')
const mouseUp$ = fromEvent(canvas, 'mouseup')
const mouseOut$ = fromEvent(canvas, 'mouseout')

function createInputStream(node) {
  return fromEvent(node, 'input')
  .pipe(
    map(e => e.target.value),
    startWith(node.value)
  )
}

const lineWidth$ = createInputStream(range)
const strokeStyle$ = createInputStream(color)


  // const strokeStyle$ = fromEvent(color, 'input')
  // .pipe(
  //   map(e => e.target.value),
  //   startWith(color.value)
  // )

//-------------
const stream$ = mouseDown$
  .pipe(
    withLatestFrom(lineWidth$, strokeStyle$, (_, lineWidth, strokeStyle) => {
      return {
        lineWidth, strokeStyle   //когда ключ и значение одинаковые - указывается одно
      }
    }),
    switchMap(options => {
      
      return mouseMove$
      .pipe(
        map(e => ({
          x: e.offsetX,
          y: e.offsetY,
          options
        })),
        pairwise(),
        takeUntil(mouseUp$),
        takeUntil(mouseOut$)
      )
    })
  )

  stream$.subscribe( ([from, to]) => {
    const {lineWidth, strokeStyle} = from.options
    ctx.lineWidth = lineWidth
    ctx.strokeStyle = strokeStyle
    // ctx.fillRect(pos.x, pos.y, 2, 2)
    ctx.beginPath()
    ctx.moveTo(from.x, from.y)
    ctx.lineTo(to.x, to.y)
    ctx.stroke()
  })