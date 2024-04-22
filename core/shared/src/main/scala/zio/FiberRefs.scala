/*
 * Copyright 2017-2024 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio

import zio.FiberRefs.Patch
import zio.FiberRefs.Patch.{Add, Remove, Update}
import zio.stacktracer.TracingImplicits.disableAutoTrace

import scala.annotation.{switch, tailrec}

/**
 * `FiberRefs` is a data type that represents a collection of `FiberRef` values.
 * This allows safely propagating `FiberRef` values across fiber boundaries, for
 * example between an asynchronous producer and consumer.
 */
// TODO Jules: Check order of declaration to match the FiberRefsMap one
sealed trait FiberRefs {
  /**
   * Returns a new fiber refs with the specified ref deleted from it.
   */
  def delete(fiberRef: FiberRef[_]): FiberRefs

  def updatedAs[A](
    fiberId: FiberId.Runtime
  )(fiberRef: FiberRef[A], value: A): FiberRefs

  /**
   * Gets the value of the specified `FiberRef` in this collection of `FiberRef`
   * values if it exists or `None` otherwise.
   */
  def get[A](fiberRef: FiberRef[A]): Option[A]

  /**
   * Gets the value of the specified `FiberRef` in this collection of `FiberRef`
   * values if it exists or the `initial` value of the `FiberRef` otherwise.
   */
  def getOrDefault[A](fiberRef: FiberRef[A]): A

  /**
   * Forks this collection of fiber refs as the specified child fiber id. This
   * will potentially modify the value of the fiber refs, as determined by the
   * individual fiber refs that make up the collection.
   */
  def forkAs(childId: FiberId.Runtime): FiberRefs

  /**
   * Joins this collection of fiber refs to the specified collection, as the
   * specified fiber id. This will perform diffing and merging to ensure
   * preservation of maximum information from both child and parent refs.
   */
  def joinAs(fiberId: FiberId.Runtime)(that: FiberRefs): FiberRefs

  def setAll(implicit trace: Trace): UIO[Unit]

  private[zio] def getOrNull[A](fibesrRef: FiberRef[A]): A

  /**
   * Constructs a patch that describes the changes between the current and the
   * passed `FiberRefs`.
   */
  private[zio] def diff(newValue: FiberRefs): Patch
}

object FiberRefs {

  /**
   * Empty FiberRefs implementation
   */
  private[zio] case object Empty extends FiberRefs { self =>
    override def delete(fiberRef: FiberRef[_]): FiberRefs = self

    override def updatedAs[A](fiberId: FiberId.Runtime)(fiberRef: FiberRef[A], value: A): FiberRefs =
      FiberRefs(fiberId, fiberRef, value)

    override def get[A](fiberRef: FiberRef[A]): Option[A] = Option.empty

    override def getOrDefault[A](fiberRef: FiberRef[A]): A = fiberRef.initial

    override def forkAs(childId: FiberId.Runtime): FiberRefs = self

    override def joinAs(fiberId: FiberId.Runtime)(that: FiberRefs): FiberRefs = that // TODO Jules: Is this correct?

    override def setAll(implicit trace: Trace): UIO[Unit] = ZIO.unit

    override private[zio] def getOrNull[A](fiberRef: FiberRef[A]): A = null.asInstanceOf[A]

    override private[zio] def diff(newValue: FiberRefs): Patch =
      newValue match {
        case Empty             => Patch.empty
        case map: FiberRefsMap =>
          // TODO Jules: Is this correct?
          map.fiberRefLocals.foldLeft(Patch.empty) { case (patch, (fiberRef, fiberRefStack)) =>
            type V = fiberRef.Value
            patch.combine(Patch.Add(fiberRef.asInstanceOf[FiberRef[V]], fiberRefStack.value.asInstanceOf[V]))
          }
      }

  }

  /**
   * Non-empty FiberRefs implementation
   */
  private[zio] final class FiberRefsMap private[FiberRefs] (
    private[FiberRefs] val fiberRefLocals: Map[FiberRef[_], FiberRefs.FiberRefStack[_]]
  ) extends FiberRefs { self =>
    assert(fiberRefLocals.nonEmpty, "`FiberRefsMap` internal map should not be empty. Use `FiberRefs.Empty` instead.")

    override def delete(fiberRef: FiberRef[_]): FiberRefs = {
      val newMap = fiberRefLocals - fiberRef
      if (newMap eq fiberRefLocals) self
      else FiberRefs(newMap)
    }

    /**
     * Returns a set of each `FiberRef` in this collection.
     */
    def fiberRefs: Set[FiberRef[_]] =
      fiberRefLocals.keySet

    /**
     * Boolean flag which indicates whether the FiberRefs map contains an entry
     * that will cause the map to be changed when [[forkAs]] is called.
     *
     * This way we can avoid calling `fiberRefLocals.transform` on every
     * invocation of [[forkAs]] when we already know that the map will not be
     * transformed
     */
    private var needsTransformWhenForked: Boolean = true

    override def forkAs(childId: FiberId.Runtime): FiberRefs =
      if (needsTransformWhenForked) {
        var modified = false
        val childMap = fiberRefLocals.transform { case (fiberRef, fiberRefStack) =>
          val oldValue = fiberRefStack.value.asInstanceOf[fiberRef.Value]
          val newValue = fiberRef.patch(fiberRef.fork)(oldValue)
          if (oldValue == newValue) fiberRefStack
          else {
            modified = true
            fiberRefStack.put(childId, newValue)
          }
        }

        if (modified) FiberRefs(childMap)
        else {
          needsTransformWhenForked = false
          self
        }
      } else self

    override def get[A](fiberRef: FiberRef[A]): Option[A] =
      Option(getOrNull(fiberRef))

    override def getOrDefault[A](fiberRef: FiberRef[A]): A =
      getOrNull(fiberRef) match {
        case null => fiberRef.initial
        case v    => v
      }

    private[zio] override def getOrNull[A](fiberRef: FiberRef[A]): A = {
      val out = fiberRefLocals.getOrElse(fiberRef, null)
      if (out eq null) null else out.value
    }.asInstanceOf[A]

    override def joinAs(fiberId: FiberId.Runtime)(that: FiberRefs): FiberRefs =
      that match {
        case FiberRefs.empty => self
        case that: FiberRefsMap =>
          val parentFiberRefs = self.fiberRefLocals
          val childFiberRefs  = that.fiberRefLocals

          @tailrec
          def findAncestor(initialValue: Any)(
            parentStack: List[FiberRefStackEntry[?]],
            parentDepth: Int,
            childStack: List[FiberRefStackEntry[?]],
            childDepth: Int
          ): Any =
            parentStack match {
              case Nil => initialValue // case (Nil, _)
              case parentHead :: parentAncestors =>
                childStack match {
                  case Nil => initialValue // case (_, Nil)
                  case childHead :: childAncestors => // case (head0 :: tail0, head1 :: tail1)
                    if (parentHead.fiberId == childHead.fiberId)
                      if (childHead.version > parentHead.version) parentHead.value else childHead.value
                    else {
                      // TODO Jules: revert "compare" change
                      ((childDepth compare parentDepth): @switch) match {
                        case 1 /* c > p */ =>
                          findAncestor(initialValue)(parentStack, parentDepth, childAncestors, childDepth - 1)
                        case -1 /* c < p */ =>
                          findAncestor(initialValue)(parentAncestors, parentDepth - 1, childStack, childDepth)
                        case 0 /* c == p */ =>
                          findAncestor(initialValue)(parentAncestors, parentDepth - 1, childAncestors, childDepth - 1)
                      }
                    }
                }
            }

          val newFiberRefLocals = {
            // for each entry in `childFiberRefs`
            childFiberRefs.foldLeft(parentFiberRefs) { case (parentFiberRefs, (ref, childStack)) =>
              if (childStack.fiberId == fiberId) {
                // if the child stack is the current fiber, we can just return the parent fiber refs
                parentFiberRefs
              } else {
                // if the child stack is not the current fiber, we need to merge the child stack with the parent stack
                val childFiberRef     = ref.asInstanceOf[FiberRef[Any]]
                val childInitialValue = childFiberRef.initial
                val childCurrentValue = childStack.value

                parentFiberRefs.getOrElse(childFiberRef, null) match {
                  case null =>
                    if (childCurrentValue == childInitialValue) parentFiberRefs
                    else
                      parentFiberRefs.updated(
                        childFiberRef,
                        FiberRefStack.init(fiberId, childFiberRef.join(childInitialValue, childCurrentValue))
                      )
                  case parentStack =>
                    val ancestor =
                      findAncestor(childInitialValue)(
                        parentStack.stack,
                        parentStack.depth,
                        childStack.stack,
                        childStack.depth
                      )
                    val patch    = childFiberRef.diff(ancestor, childCurrentValue)
                    val oldValue = parentStack.value
                    val newValue = childFiberRef.join(oldValue, childFiberRef.patch(patch)(oldValue))

                    if (oldValue == newValue) parentFiberRefs
                    else {
                      val newEntry =
                        if (parentStack.fiberId == fiberId) parentStack.updateValue(newValue)
                        else parentStack.put(fiberId, newValue)

                      parentFiberRefs.updated(childFiberRef, newEntry)
                    }
                }
              }
            }
          }

          if (self.fiberRefLocals eq newFiberRefLocals) self
          else FiberRefs(newFiberRefLocals)
      }

    override def setAll(implicit trace: Trace): UIO[Unit] =
      ZIO.foreachDiscard(fiberRefs) { fiberRef =>
        fiberRef.asInstanceOf[FiberRef[Any]].set(getOrDefault(fiberRef))
      }

    override def toString: String = fiberRefLocals.mkString("FiberRefLocals(", ",", ")")

    override def updatedAs[A](fiberId: FiberId.Runtime)(fiberRef: FiberRef[A], value: A): FiberRefs = {
      val oldStack = fiberRefLocals.getOrElse(fiberRef, null)

      val newStack =
        if (oldStack eq null) FiberRefStack.init(fiberId, value)
        else {
          if (oldStack.fiberId == fiberId) {
            if (oldStack.value == value) oldStack else oldStack.updateValue(value)
          } else if (oldStack.value == value) oldStack
          else oldStack.put(fiberId, value)
        }

      if (oldStack eq newStack) self
      else FiberRefs(fiberRefLocals.updated(fiberRef, newStack))
    }

    override private[zio] def diff(newValue: FiberRefs): Patch =
      newValue match {
        case Empty => Patch.empty // TODO Jules: Is this correct? Or should we `Patch.Revome` everything?
        case newValue0: FiberRefsMap =>
          val (removed, patch) = newValue0.fiberRefLocals.foldLeft[(FiberRefs, Patch)](self -> Patch.empty) {
            case ((fiberRefs, patch), (fiberRef, fiberRefStack)) =>
              type V = fiberRef.Value
              val newValue0 = fiberRefStack.value.asInstanceOf[V]

              fiberRefs.getOrNull(fiberRef) match {
                case null => fiberRefs -> patch.combine(Add(fiberRef.asInstanceOf[FiberRef[V]], newValue0))
                case oldValue =>
                  val patch0 =
                    if (oldValue == newValue0) patch
                    else {
                      patch.combine(
                        Update(
                          fiberRef.asInstanceOf[FiberRef.WithPatch[V, fiberRef.Patch]],
                          fiberRef.diff(oldValue.asInstanceOf[V], newValue0)
                        )
                      )
                    }

                  fiberRefs.delete(fiberRef) -> patch0
              }
          }

          removed
            .asInstanceOf[FiberRefsMap]
            .fiberRefLocals
            .foldLeft(patch) { case (patch, (fiberRef, _)) => patch.combine(Remove(fiberRef)) }
      }
  }

  private[zio] final case class FiberRefStackEntry[A](
    fiberId: FiberId.Runtime,
    value: A,
    version: Int
  )

  /**
   * The 'head' of the stack is inlined in the `FiberRefStack` data structure:
   *
   * Instead of having
   * {{{
   *   FiberRefStack(
   *     head: FiberRefStackEntry[?],
   *     tail: List[FiberRefStackEntry[?],
   *     depth: Int
   *   )
   * }}}
   * we inline the content of the `head: FiberRefStackEntry` data structure
   * directly into the `FiberRefStack` to avoid to have to instantiate this
   * `FiberRefStackEntry` as much as possible.
   */
  private[zio] final case class FiberRefStack[A] private[FiberRefs] (
    private val headFiberId: FiberId.Runtime, // private so that we can use an explicit internal naming and a cleaner external API
    private val headValue: A,                 // private so that we can use an explicit internal naming and a cleaner external API
    private val headVersion: Int,             // private so that we can use an explicit internal naming and a cleaner external API
    tail: List[FiberRefStackEntry[?]],
    depth: Int
  ) {
    // nicer names for these variables when used outside of the FiberRefStack
    def fiberId: FiberId.Runtime = headFiberId // should be `@inline def` but https://github.com/scala/bug/issues/12988
    def value: A                 = headValue   // should be `@inline def` but https://github.com/scala/bug/issues/12988
    def version: Int             = headVersion // should be `@inline def` but https://github.com/scala/bug/issues/12988

    @inline private def head: FiberRefStackEntry[A] = FiberRefStackEntry(headFiberId, headValue, headVersion)
    @inline def stack: List[FiberRefStackEntry[?]]  = head :: tail

    /**
     * Update the value of the head entry
     */
    @inline def updateValue(newValue: Any): FiberRefStack[?] =
      FiberRefStack(
        headFiberId = headFiberId,
        headValue = newValue,
        headVersion = headVersion + 1,
        tail = tail,
        depth = depth
      )

    /**
     * Add a new entry on top of the Stack
     */
    @inline def put(fiberId: FiberId.Runtime, value: Any): FiberRefStack[?] =
      FiberRefStack(
        headFiberId = fiberId,
        headValue = value,
        headVersion = 0,
        tail = stack,
        depth = depth + 1
      )
  }
  private[zio] object FiberRefStack {
    @inline def init[A](fiberId: FiberId.Runtime, value: A): FiberRefStack[?] =
      FiberRefStack(headFiberId = fiberId, headValue = value, headVersion = 0, tail = List.empty, depth = 1)
  }

  /**
   * The empty collection of `FiberRef` values.
   */
  val empty: FiberRefs = FiberRefs.Empty

  private[zio] def apply[A](fiberId: FiberId.Runtime, fiberRef: FiberRef[A], initialValue: A): FiberRefs =
    new FiberRefsMap(Map(fiberRef -> FiberRefStack.init(fiberId, initialValue)))

  private[zio] def apply(fiberRefLocals: Map[FiberRef[_], FiberRefStack[_]]): FiberRefs =
    if (fiberRefLocals.isEmpty) FiberRefs.empty else new FiberRefsMap(fiberRefLocals)

  /**
   * A `Patch` captures the changes in `FiberRef` values made by a single fiber
   * as a value. This allows fibers to apply the changes made by a workflow
   * without inheriting all the `FiberRef` values of the fiber that executed the
   * workflow.
   */
  sealed trait Patch extends ((FiberId.Runtime, FiberRefs) => FiberRefs) { self =>
    import Patch._

    /**
     * Applies the changes described by this patch to the specified collection
     * of `FiberRef` values.
     */
    final def apply(fiberId: FiberId.Runtime, fiberRefs: FiberRefs): FiberRefs = {

      @tailrec
      def loop(fiberRefs: FiberRefs, patches: List[Patch]): FiberRefs =
        patches match {
          case Add(fiberRef, value) :: patches =>
            loop(fiberRefs.updatedAs(fiberId)(fiberRef, value), patches)
          case AndThen(first, second) :: patches =>
            loop(fiberRefs, first :: second :: patches)
          case Patch.Empty :: patches =>
            loop(fiberRefs, patches)
          case Remove(fiberRef) :: patches =>
            loop(fiberRefs.delete(fiberRef), patches)
          case Update(fiberRef, patch) :: patches =>
            loop(
              fiberRefs.updatedAs(fiberId)(fiberRef, fiberRef.patch(patch)(fiberRefs.getOrDefault(fiberRef))),
              patches
            )
          case Nil =>
            fiberRefs
        }

      loop(fiberRefs, List(self))
    }

    /**
     * Combines this patch and the specified patch to create a new patch that
     * describes applying the changes from this patch and the specified patch
     * sequentially.
     */
    final def combine(that: Patch): Patch = AndThen(self, that)
  }

  object Patch {

    /**
     * The empty patch that describes no changes to `FiberRef` values.
     */
    val empty: Patch = Patch.Empty

    private[FiberRefs] final case class Add[Value0](fiberRef: FiberRef[Value0], value: Value0) extends Patch
    private[FiberRefs] final case class AndThen(first: Patch, second: Patch)                   extends Patch
    private[FiberRefs] case object Empty                                                       extends Patch
    private[FiberRefs] final case class Remove[Value0](fiberRef: FiberRef[Value0])             extends Patch
    private[FiberRefs] final case class Update[Value0, Patch0](
      fiberRef: FiberRef.WithPatch[Value0, Patch0],
      patch: Patch0
    ) extends Patch
  }
}
